require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const http = require('http');
const path = require('path');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { body, param, validationResult } = require('express-validator');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Models
const Stream = require('./models/Stream');
const AuditLog = require('./models/AuditLog');
const Visitor = require('./models/Visitor');
const MetricsHistory = require('./models/MetricsHistory');

// ===== SECURITY MIDDLEWARE =====

// Helmet - Secure HTTP headers
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
            fontSrc: ["'self'", "https://fonts.gstatic.com"],
            imgSrc: ["'self'", "data:", "blob:", "*"],
            scriptSrc: ["'self'", "'unsafe-inline'"],
            connectSrc: ["'self'", "ws:", "wss:", "*"]
        }
    },
    crossOriginEmbedderPolicy: false
}));

// Rate Limiting - Prevent brute force attacks
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    message: { error: 'Too many requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false
});

// Stricter rate limit for adding streams (prevent spam)
const addStreamLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 5, // Only 5 stream additions per minute
    message: { error: 'Too many streams added. Please wait a minute.' }
});

// Apply rate limiting to API routes
app.use('/api/', apiLimiter);

// Request size limits - Prevent large payload attacks
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// CORS with restricted origin in production (currently open for development)
app.use(cors());

// MongoDB Connection (Restored)
// MongoDB Connection
const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
    console.error('❌ FATAL: MONGO_URI environment variable is not defined.');
    process.exit(1);
}

console.log('🔗 Connecting to Atlas MongoDB...');
mongoose.connect(MONGO_URI)
    .then(async () => {
        console.log('✅ MongoDB Connected Successfully!');
        try {
            const count = await Stream.countDocuments();
            console.log(`📊 Startup Check: Found ${count} streams in DB.`);
        } catch (e) {
            console.error('⚠️ DB Check Warning:', e.message);
        }
    })
    .catch(err => console.error('❌ MongoDB Connection Error:', err));

// Trust proxy for ngrok

// Parse User-Agent to get device name using ua-parser-js
const UAParser = require('ua-parser-js');
const geoip = require('geoip-lite');

function parseDeviceName(userAgent) {
    if (!userAgent) return 'Unknown Device';

    const parser = new UAParser(userAgent);
    const result = parser.getResult();

    const browser = result.browser.name || 'Unknown Browser';
    const os = result.os.name || 'Unknown OS';
    const device = result.device.model || result.device.type || '';

    // Format: "Chrome on Windows 10" or "Safari on iPhone 14"
    if (device) {
        return `${browser} on ${device}`;
    }
    return `${browser} on ${os}`;
}

// Audit Logger Helper
async function logAction(action, streamData, req) {
    try {
        const userAgent = req.get('User-Agent');
        await AuditLog.create({
            action,
            streamId: streamData._id || streamData.id,
            streamName: streamData.name,
            streamUrl: streamData.url,
            details: streamData.details || null,
            ipAddress: req.ip || req.connection?.remoteAddress,
            userAgent: userAgent,
            deviceName: parseDeviceName(userAgent)
        });
    } catch (err) {
        console.error('Audit log error:', err.message);
    }
}

// Serve frontend static files (production build)
const frontendPath = path.join(__dirname, '../frontend/dist');
app.use(express.static(frontendPath));

// API root
app.get('/api', (req, res) => {
    res.json({ message: 'HLS Monitor API is running' });
});

// ===== STREAM ROUTES =====

// ObjectId validation middleware - prevents crashes from bad IDs
const validateObjectId = (req, res, next) => {
    if (req.params.id && !mongoose.Types.ObjectId.isValid(req.params.id)) {
        return res.status(400).json({ error: 'Invalid stream ID format' });
    }
    next();
};

// Input validation for adding streams
const validateStream = [
    body('url')
        .isURL({ protocols: ['http', 'https'], require_tld: false, require_protocol: true })
        .withMessage('Invalid URL format')
        .isLength({ max: 500 })
        .withMessage('URL too long'),
    body('name')
        .trim()
        .isLength({ min: 1, max: 100 })
        .withMessage('Name must be 1-100 characters')
        .escape() // Sanitize HTML
];

// Add stream (with rate limiting and validation)
app.post('/api/streams', addStreamLimiter, validateStream, async (req, res) => {
    try {
        // Check validation errors
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({ error: errors.array()[0].msg });
        }

        const { url, name } = req.body;
        const stream = new Stream({ url, name });
        await stream.save();

        // Log the action
        await logAction('STREAM_ADDED', stream, req);

        // Emit real-time event
        io.emit('stream:added', stream);

        res.status(201).json(stream);
    } catch (err) {
        res.status(400).json({ error: err.message });
    }
});

// Get all streams
app.get('/api/streams', async (req, res) => {
    try {
        const streams = await Stream.find().sort({ createdAt: -1 });
        res.json(streams);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get single stream (exclude errors - fetched separately via pagination)
app.get('/api/streams/:id', validateObjectId, async (req, res) => {
    try {
        const stream = await Stream.findById(req.params.id).select('-streamErrors');
        if (!stream) return res.status(404).json({ error: 'Not found' });
        res.json(stream);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get paginated errors for a stream (lazy loading)
app.get('/api/streams/:id/errors', validateObjectId, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const skip = (page - 1) * limit;

        const stream = await Stream.findById(req.params.id).select('streamErrors');
        if (!stream) return res.status(404).json({ error: 'Not found' });

        const allErrors = stream.streamErrors || [];
        const total = allErrors.length;

        // Reverse to get newest first, then paginate
        const errors = allErrors.slice().reverse().slice(skip, skip + limit);

        res.json({
            errors,
            total,
            page,
            totalPages: Math.ceil(total / limit),
            hasMore: skip + limit < total
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Delete stream - SECURE
app.delete('/api/streams/:id', validateObjectId, async (req, res) => {
    try {
        const { confirmation } = req.body;

        // Security Check
        if (confirmation !== 'CONFIRM DELETE STREAM') {
            return res.status(401).json({ error: 'Security phrase incorrect. Deletion aborted.' });
        }

        const stream = await Stream.findById(req.params.id);
        if (!stream) return res.status(404).json({ error: 'Not found' });

        // Log before deleting
        await logAction('STREAM_DELETED', stream, req);

        await Stream.findByIdAndDelete(req.params.id);

        // Emit real-time event
        io.emit('stream:deleted', req.params.id);

        res.json({ message: 'Stream deleted' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get available log dates (last 7 days)
app.get('/api/streams/:id/logs/dates', validateObjectId, async (req, res) => {
    try {
        const stream = await Stream.findById(req.params.id).select('streamErrors');
        if (!stream) return res.status(404).json({ error: 'Not found' });

        // Generate last 7 days
        const dates = [];
        const today = new Date();
        for (let i = 0; i < 7; i++) {
            const date = new Date(today);
            date.setDate(date.getDate() - i);
            const dateStr = date.toISOString().split('T')[0];

            // Count errors for this date
            const errorCount = (stream.streamErrors || []).filter(err => {
                if (!err.date) return false;
                return new Date(err.date).toISOString().split('T')[0] === dateStr;
            }).length;

            dates.push({ date: dateStr, errorCount });
        }

        res.json(dates);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Download stream log (Human-Readable Format)
app.get('/api/streams/:id/log', validateObjectId, async (req, res) => {
    try {
        const stream = await Stream.findById(req.params.id);
        if (!stream) return res.status(404).json({ error: 'Not found' });

        // Log the download action
        await logAction('LOG_DOWNLOADED', {
            ...stream.toObject(),
            details: `Log file downloaded for stream: ${stream.name}`
        }, req);

        const health = stream.health || {};
        const stats = stream.stats || {};
        let errors = stream.streamErrors || [];

        // Filter by date if provided
        const dateFilter = req.query.date;
        let dateTitle = "FULL LOG HISTORY";

        if (dateFilter) {
            dateTitle = `LOG FOR ${dateFilter}`;
            errors = errors.filter(err => {
                if (!err.date) return false;
                return new Date(err.date).toISOString().split('T')[0] === dateFilter;
            });
        }

        // Build human-readable log
        let log = `
╔════════════════════════════════════════════════════════════════════╗
║                    HLS MONITOR - STREAM LOG                        ║
╚════════════════════════════════════════════════════════════════════╝

📺 STREAM INFORMATION
─────────────────────────────────────────────────────────────────────
  Name:           ${stream.name}
  URL:            ${stream.url}
  Status:         ${stream.status?.toUpperCase() || 'UNKNOWN'}
  Export Date:    ${new Date().toLocaleString()}
  Log Period:     ${dateTitle}

📊 HEALTH METRICS
─────────────────────────────────────────────────────────────────────
  Is Stale:           ${health.isStale ? 'YES ⚠️' : 'NO ✅'}
  Media Sequence:     ${health.mediaSequence ?? 'N/A'}
  Segment Count:      ${health.segmentCount ?? 'N/A'}
  Target Duration:    ${health.targetDuration ? health.targetDuration + 's' : 'N/A'}
  Playlist Type:      ${health.playlistType || 'N/A'}
  
  Sequence Jumps:     ${health.sequenceJumps ?? 0}
  Sequence Resets:    ${health.sequenceResets ?? 0}
  Discontinuities:    ${health.discontinuityCount ?? 0}
  Total Errors:       ${health.totalErrors ?? 0}

🎬 VIDEO STREAM
─────────────────────────────────────────────────────────────────────
  Codec:          ${stats.video?.codec || 'N/A'}
  Profile:        ${stats.video?.profile || 'N/A'}
  Level:          ${stats.video?.level || 'N/A'}
  Resolution:     ${stats.resolution || 'N/A'}
  FPS:            ${stats.fps?.toFixed(2) || 'N/A'}
  Pixel Format:   ${stats.video?.pixFmt || 'N/A'}
  Color Space:    ${stats.video?.colorSpace || 'N/A'}
  Video Bitrate:  ${stats.video?.bitRate ? (stats.video.bitRate / 1000).toFixed(0) + ' kbps' : 'N/A'}

🔊 AUDIO STREAM
─────────────────────────────────────────────────────────────────────
  Codec:          ${stats.audio?.codec || 'N/A'}
  Channels:       ${stats.audio?.channels || 'N/A'}
  Sample Rate:    ${stats.audio?.sampleRate ? stats.audio.sampleRate + ' Hz' : 'N/A'}
  Audio Bitrate:  ${stats.audio?.bitRate ? (stats.audio.bitRate / 1000).toFixed(0) + ' kbps' : 'N/A'}

📦 CONTAINER INFO
─────────────────────────────────────────────────────────────────────
  Format:         ${stats.container?.formatName || 'N/A'}
  Duration:       ${stats.container?.duration ? stats.container.duration.toFixed(2) + 's' : 'N/A'}
  Size:           ${stats.container?.size ? (stats.container.size / 1024).toFixed(1) + ' KB' : 'N/A'}
  Total Bitrate:  ${stats.container?.bitRate ? (stats.container.bitRate / 1000).toFixed(0) + ' kbps' : 'N/A'}
  Bandwidth:      ${stats.bandwidth ? (stats.bandwidth / 1000000).toFixed(2) + ' Mbps' : 'N/A'}

📅 TIMESTAMPS
─────────────────────────────────────────────────────────────────────
  Created:        ${stream.createdAt ? new Date(stream.createdAt).toLocaleString() : 'N/A'}
  Last Updated:   ${stream.updatedAt ? new Date(stream.updatedAt).toLocaleString() : 'N/A'}
  Last Checked:   ${stream.lastChecked ? new Date(stream.lastChecked).toLocaleString() : 'N/A'}

`;

        // Add errors section if any
        if (errors.length > 0) {
            log += `⚠️  ERROR LOG (${errors.length} errors)
─────────────────────────────────────────────────────────────────────
`;
            errors.forEach((err, i) => {
                log += `
  [${i + 1}] ${err.errorType || 'Unknown Error'}
      Time:    ${err.date ? new Date(err.date).toLocaleString() : 'N/A'}
      Details: ${err.details || 'No details'}
      Type:    ${err.mediaType || 'N/A'}
`;
            });
        } else {
            log += `✅ NO ERRORS RECORDED
─────────────────────────────────────────────────────────────────────
  This stream has no recorded errors.
`;
        }

        log += `
═════════════════════════════════════════════════════════════════════
                    Generated by HLS Monitor
═════════════════════════════════════════════════════════════════════
`;

        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        const filenameDate = dateFilter || new Date().toISOString().split('T')[0];
        res.setHeader('Content-Disposition', `attachment; filename="${stream.name.replace(/[^a-z0-9]/gi, '_')}_log_${filenameDate}.txt"`);
        res.send(log);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ===== METRICS HISTORY ROUTES =====

// Get metrics history for a stream (for graphs) - returns ALL data from start
app.get('/api/streams/:id/metrics', validateObjectId, async (req, res) => {
    try {
        const metrics = await MetricsHistory.find({ streamId: req.params.id })
            .sort({ timestamp: 1 });  // No limit - get all historical data
        res.json(metrics);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ===== AUDIT LOG ROUTES =====

// Get all audit logs
app.get('/api/audit-logs', async (req, res) => {
    try {
        const logs = await AuditLog.find().sort({ timestamp: -1 }).limit(100);
        res.json(logs);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get audit logs by action type
app.get('/api/audit-logs/:action', async (req, res) => {
    try {
        const logs = await AuditLog.find({ action: req.params.action.toUpperCase() })
            .sort({ timestamp: -1 })
            .limit(50);
        res.json(logs);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ===== VISITOR TRACKING =====

app.post('/api/visitors', async (req, res) => {
    try {
        const { visitorId, screen, metadata, name } = req.body;

        // Basic Validation
        if (!visitorId) {
            return res.status(400).json({ success: false, error: 'Missing visitorId' });
        }

        const userAgent = req.get('User-Agent') || '';
        let ip = req.ip || req.connection?.remoteAddress || '';

        // Handle localhost/proxy IP - Normalize
        if (ip === '::1' || ip === '127.0.0.1') {
            // Localhost
        }

        // Clean IP (remove IPv6 prefix if present)
        if (ip && ip.includes("::ffff:")) {
            ip = ip.replace("::ffff:", "");
        }

        // Get Location (Safe Lookup)
        let locationData = {};
        try {
            if (ip && ip.length > 3) { // valid IP check
                const geo = geoip.lookup(ip);
                if (geo) {
                    locationData = {
                        country: geo.country,
                        region: geo.region,
                        city: geo.city,
                        ll: geo.ll,
                        timezone: geo.timezone
                    };
                }
            }
        } catch (geoErr) {
            console.warn('GeoIP Lookup Error:', geoErr.message);
        }

        // Parse User Agent (Safe Parse)
        let deviceData = {};
        try {
            const parser = new UAParser(userAgent);
            const deviceResult = parser.getResult();
            deviceData = {
                browser: deviceResult.browser.name,
                browserVersion: deviceResult.browser.version,
                os: deviceResult.os.name,
                osVersion: deviceResult.os.version,
                deviceType: deviceResult.device.type || 'desktop',
                deviceModel: deviceResult.device.model,
                deviceVendor: deviceResult.device.vendor,
                cpuArchitecture: deviceResult.cpu.architecture
            };
        } catch (uaErr) {
            console.warn('UA Parse Error:', uaErr.message);
        }

        // Upsert Visitor
        const visitor = await Visitor.findOneAndUpdate(
            { visitorId: visitorId },
            {
                $set: {
                    ip: ip,
                    location: locationData,
                    device: deviceData,
                    screen: screen || {},
                    metadata: metadata || {},
                    name: name || 'Guest',
                    lastVisit: new Date()
                },
                $inc: { visitCount: 1 },
                $setOnInsert: { firstVisit: new Date() }
            },
            { new: true, upsert: true, setDefaultsOnInsert: true }
        );

        res.json({ success: true, visitorId: visitor.visitorId });
    } catch (err) {
        console.error("Visitor tracking error:", err);
        // Don't block the client if tracking fails
        res.status(200).json({ success: false, error: err.message });
    }
});

// Socket.io
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

// Start Workers
require('./workers/monitor')(io);

// SPA fallback - serve index.html for all non-API routes
app.use((req, res, next) => {
    if (!req.path.startsWith('/api') && !req.path.startsWith('/socket.io')) {
        res.sendFile(path.join(__dirname, '../frontend/dist/index.html'));
    } else {
        next();
    }
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

// ===== GLOBAL SAFETY NET =====

// Prevent crash on unhandled rejection (Promise error)
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Prevent crash on uncaught exception
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception thrown:', err);
});
