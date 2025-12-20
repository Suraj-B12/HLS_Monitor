const axios = require('axios');
const m3u8Parser = require('m3u8-parser');
const Stream = require('../models/Stream');
const MetricsHistory = require('../models/MetricsHistory');
const { ErrorTypes } = require('../models/Stream');
const { processSegment } = require('./processor');
const { v4: uuidv4 } = require('uuid');

const MONITOR_INTERVAL = 7000; // 7 seconds as requested
const SLIDING_WINDOW_SIZE = 100; // Last 100 segments (~12 minutes)

// Error decay factor based on time since last error
// Returns a value 0-1 where 1 = full forgiveness, 0 = no forgiveness
// Always returns a safe number, never throws
function getErrorDecayFactor(lastErrorTime) {
    try {
        // No errors ever = full forgiveness
        if (!lastErrorTime) return 1.0;

        // Parse the date safely
        const errorDate = new Date(lastErrorTime);
        const errorTimestamp = errorDate.getTime();

        // If parsing failed, assume no decay (be conservative)
        if (isNaN(errorTimestamp)) return 0;

        const hoursSinceError = (Date.now() - errorTimestamp) / (1000 * 60 * 60);

        // Handle negative or invalid values (future dates, clock issues)
        if (hoursSinceError < 0 || !isFinite(hoursSinceError)) return 0;

        // Progressive forgiveness timeline
        if (hoursSinceError < 1) return 0;        // First hour: Full penalty
        if (hoursSinceError < 6) return 0.25;     // 1-6 hours: 25% forgiveness
        if (hoursSinceError < 24) return 0.5;     // 6-24 hours: 50% forgiveness
        if (hoursSinceError < 72) return 0.75;    // 1-3 days: 75% forgiveness
        return 0.9;                                // 3+ days: 90% forgiveness
    } catch (err) {
        // On any error, return 0 (no forgiveness) to be conservative
        return 0;
    }
}

// Calculate sliding window metrics from recent streamErrors
// Counts errors that occurred within the last WINDOW_MINUTES
async function calculateSlidingWindowMetrics(streamId) {
    const defaultResult = { jumps: 0, resets: 0, errors: 0 };

    if (!streamId) return defaultResult;

    try {
        // Get stream with recent errors
        const Stream = require('../models/Stream');
        const stream = await Stream.findById(streamId).select('streamErrors').lean();

        if (!stream || !stream.streamErrors) return defaultResult;

        // Window: last 12 minutes (100 segments × ~7 seconds)
        const windowStart = new Date(Date.now() - 12 * 60 * 1000);

        // Filter errors within the window
        const recentErrors = stream.streamErrors.filter(err => {
            if (!err.date) return false;
            return new Date(err.date) >= windowStart;
        });

        // Count by type
        let jumps = 0;
        let resets = 0;
        let errors = 0;

        recentErrors.forEach(err => {
            errors++;
            if (err.errorType === 'SEQUENCE_JUMP' ||
                (err.details && err.details.includes('Sequence jumped'))) {
                jumps++;
            }
            if (err.errorType === 'SEQUENCE_RESET' ||
                (err.details && err.details.includes('reset'))) {
                resets++;
            }
        });

        return { jumps, resets, errors };
    } catch (err) {
        console.error(`[SLIDING] Error calculating metrics: ${err.message}`);
        return defaultResult;
    }
}

// Calculate health score (0-100) with sliding window + decay
function calculateHealthScore(stream, recentIssues = null, decayFactor = 0) {
    let score = 100;
    const health = stream.health || {};

    // Immediate penalties (current status)
    if (health.isStale) score -= 30;
    if (stream.status === 'error') score -= 40;
    if (stream.status === 'offline') score -= 50;

    // If we have sliding window data, use it with decay
    if (recentIssues) {
        const effectiveDecay = 1 - decayFactor; // Convert forgiveness to penalty multiplier

        // Apply penalties with decay factor
        const jumpPenalty = Math.min(recentIssues.jumps * 5, 20) * effectiveDecay;
        const resetPenalty = Math.min(recentIssues.resets * 10, 30) * effectiveDecay;
        const errorPenalty = Math.min(recentIssues.errors * 2, 20) * effectiveDecay;

        score -= (jumpPenalty + resetPenalty + errorPenalty);
    } else {
        // Fallback to all-time metrics (for backwards compatibility)
        if (health.sequenceJumps > 0) score -= Math.min(health.sequenceJumps * 5, 20);
        if (health.sequenceResets > 0) score -= Math.min(health.sequenceResets * 10, 30);
        if (health.totalErrors > 0) score -= Math.min(health.totalErrors * 2, 20);
    }

    return Math.max(0, Math.min(100, score));
}

// Calculate video/audio scores based on stream quality
function calculateVideoScore(stream) {
    let score = 100;
    const video = stream.stats?.video;
    if (!video) return 50;
    if (!video.codec) score -= 20;
    if (video.width && video.width < 720) score -= 10;
    if (video.width && video.width >= 1920) score += 0;
    return Math.max(0, Math.min(100, score));
}

function calculateAudioScore(stream) {
    let score = 100;
    const audio = stream.stats?.audio;
    if (!audio) return 50;
    if (!audio.codec) score -= 20;
    if (audio.sampleRate && audio.sampleRate < 44100) score -= 10;
    // Penalize if audio is silent
    if (audio.isSilent) score -= 15;
    return Math.max(0, Math.min(100, score));
}
const streamState = new Map();

function generateErrorId() {
    return `eid-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

function addError(stream, errorType, details, mediaType = 'VIDEO', code = null) {
    const error = {
        eid: generateErrorId(),
        date: new Date(),
        errorType,
        mediaType,
        variant: stream.stats?.bandwidth?.toString() || 'unknown',
        details,
        code
    };

    if (!stream.streamErrors) stream.streamErrors = [];
    stream.streamErrors.push(error);
    stream.health.totalErrors++;
    stream.health.timeSinceLastError = 0;
    stream.health.lastErrorTime = new Date(); // Track for decay calculation

    console.log(`[ERROR] ${stream.name}: ${errorType} - ${details}`);
}

async function fetchManifest(url) {
    const response = await axios.get(url, { timeout: 10000 });
    const parser = new m3u8Parser.Parser();
    parser.push(response.data);
    parser.end();
    return parser.manifest;
}

async function resolveVariantUrl(masterUrl, variantUri) {
    if (variantUri.startsWith('http')) return variantUri;
    const baseUrl = masterUrl.substring(0, masterUrl.lastIndexOf('/') + 1);
    return baseUrl + variantUri;
}

async function checkStream(stream, io) {
    const now = Date.now();
    const state = streamState.get(stream._id.toString()) || {
        lastPollTime: 0,
        lastMediaSequence: -1,
        consecutiveStales: 0
    };

    try {
        // --- FETCH MANIFEST ---
        let manifest;
        let variantUrl = stream.url;

        try {
            manifest = await fetchManifest(stream.url);
        } catch (err) {
            addError(stream, ErrorTypes.MANIFEST_RETRIEVAL,
                `Failed to fetch manifest: ${err.message}`, 'MASTER', err.response?.status);
            stream.status = 'error';
            try {
                await stream.save();
            } catch (saveErr) {
                if (saveErr.name === 'VersionError') {
                    console.warn(`[WARN] ${stream.name}: VersionError during status update - skipping`);
                    return;
                }
                throw saveErr;
            }
            io.emit('stream:update', stream);
            return;
        }

        // --- HANDLE MASTER PLAYLIST ---
        if (manifest.playlists && manifest.playlists.length > 0) {
            // It's a master, follow first variant
            const variant = manifest.playlists[0];
            variantUrl = await resolveVariantUrl(stream.url, variant.uri);

            // Update bandwidth from master
            if (variant.attributes?.BANDWIDTH) {
                stream.stats.bandwidth = variant.attributes.BANDWIDTH;
            }
            if (variant.attributes?.RESOLUTION) {
                stream.stats.resolution = `${variant.attributes.RESOLUTION.width}x${variant.attributes.RESOLUTION.height}`;
            }

            try {
                manifest = await fetchManifest(variantUrl);
            } catch (err) {
                addError(stream, ErrorTypes.MANIFEST_RETRIEVAL,
                    `Failed to fetch variant: ${err.message}`, 'VIDEO', err.response?.status);
                stream.status = 'error';
                try {
                    await stream.save();
                } catch (saveErr) {
                    if (saveErr.name === 'VersionError') {
                        console.warn(`[WARN] ${stream.name}: VersionError during variant update - skipping`);
                        return;
                    }
                    throw saveErr;
                }
                io.emit('stream:update', stream);
                return;
            }
        }

        // --- ANALYZE MEDIA PLAYLIST ---
        if (!manifest.segments || manifest.segments.length === 0) {
            addError(stream, ErrorTypes.PLAYLIST_CONTENT, 'Playlist has no segments');
            stream.status = 'error';
            try {
                await stream.save();
            } catch (saveErr) {
                if (saveErr.name === 'VersionError') {
                    console.warn(`[WARN] ${stream.name}: VersionError during playlist check - skipping`);
                    return;
                }
                throw saveErr;
            }
            io.emit('stream:update', stream);
            return;
        }

        const currentSequence = manifest.mediaSequence || 0;
        const segmentCount = manifest.segments.length;
        const targetDuration = manifest.targetDuration || 0;

        // --- STALENESS CHECK ---
        if (currentSequence === state.lastMediaSequence) {
            state.consecutiveStales++;
            stream.health.timeSinceLastUpdate = now - state.lastPollTime;

            if (stream.health.timeSinceLastUpdate > stream.health.staleThreshold) {
                stream.health.isStale = true;
                stream.status = 'stale';
                addError(stream, ErrorTypes.STALE_MANIFEST,
                    `Playlist stale for ${stream.health.timeSinceLastUpdate}ms`);
            }
        } else {
            // Playlist updated
            stream.health.isStale = false;
            stream.health.lastManifestUpdate = new Date();
            stream.health.timeSinceLastUpdate = 0;
            state.consecutiveStales = 0;
            stream.status = 'online';
        }

        // --- SEQUENCE CHECKS ---
        if (state.lastMediaSequence !== -1) {
            const expectedSequence = state.lastMediaSequence + 1;

            // Check for sequence jump (gap) - only count significant gaps (3+)
            // Gaps of 1-2 are normal due to poll timing (7s) vs segment duration (~6s)
            if (currentSequence > expectedSequence) {
                const gap = currentSequence - expectedSequence;
                if (gap >= 3) {
                    stream.health.sequenceJumps++;
                    addError(stream, ErrorTypes.MEDIA_SEQUENCE,
                        `Sequence jumped from ${state.lastMediaSequence} to ${currentSequence} (gap: ${gap})`);
                }
            }

            // Check for sequence reset
            if (currentSequence < state.lastMediaSequence) {
                stream.health.sequenceResets++;
                addError(stream, ErrorTypes.MEDIA_SEQUENCE,
                    `Sequence reset from ${state.lastMediaSequence} to ${currentSequence}`);
            }
        }

        // --- DISCONTINUITY CHECK ---
        let currentDiscontinuityCount = 0;
        manifest.segments.forEach(seg => {
            if (seg.discontinuity) currentDiscontinuityCount++;
        });

        if (manifest.discontinuitySequence !== undefined) {
            if (stream.health.discontinuitySequence !== manifest.discontinuitySequence) {
                stream.health.discontinuitySequence = manifest.discontinuitySequence;
            }
        }
        stream.health.discontinuityCount = currentDiscontinuityCount;

        // --- UPDATE HEALTH ---
        stream.health.previousMediaSequence = state.lastMediaSequence;
        stream.health.mediaSequence = currentSequence;
        stream.health.segmentCount = segmentCount;
        stream.health.targetDuration = targetDuration;
        stream.health.playlistType = manifest.playlistType || 'LIVE';

        // Update state
        state.lastMediaSequence = currentSequence;
        state.lastPollTime = now;
        streamState.set(stream._id.toString(), state);

        // --- TRIGGER SPRITE GENERATION ---
        // Always process the latest segment for sprite
        const latestSegment = manifest.segments[manifest.segments.length - 1];
        let segmentUrl = latestSegment.uri;
        if (!segmentUrl.startsWith('http')) {
            const baseUrl = variantUrl.substring(0, variantUrl.lastIndexOf('/') + 1);
            segmentUrl = baseUrl + segmentUrl;
        }

        processSegment(stream, segmentUrl, io);

        // Update timestamp
        stream.lastChecked = new Date();

        try {
            await stream.save();
        } catch (saveErr) {
            if (saveErr.name === 'VersionError') {
                console.warn(`[WARN] ${stream.name}: VersionError during main loop save - skipping`);
                return;
            }
            throw saveErr;
        }

        // Calculate signal levels for graphs
        const videoBitrate = stream.stats?.video?.bitRate || stream.stats?.container?.bitRate * 0.85 || 0;
        const audioBitrate = stream.stats?.audio?.bitRate || 128000;
        const videoLevel = Math.min(100, Math.max(0, (videoBitrate / 5000000) * 100));
        const audioLevel = Math.min(100, Math.max(0, (audioBitrate / 320000) * 100));

        // Calculate sliding window metrics + decay for health score
        const recentIssues = await calculateSlidingWindowMetrics(stream._id);
        const decayFactor = getErrorDecayFactor(stream.health.lastErrorTime);

        // Update stream's recent metrics for frontend display
        stream.health.recentErrors = recentIssues.errors;
        stream.health.recentSequenceJumps = recentIssues.jumps;
        stream.health.recentSequenceResets = recentIssues.resets;

        // Record metrics history for graphs (runs in background for ALL streams)
        try {
            await MetricsHistory.create({
                streamId: stream._id,
                healthScore: calculateHealthScore(stream, recentIssues, decayFactor),
                videoScore: calculateVideoScore(stream),
                audioScore: calculateAudioScore(stream),
                videoBitrate: videoBitrate,
                audioBitrate: audioBitrate,
                videoLevel: videoLevel,
                audioLevel: audioLevel,
                fps: stream.stats?.fps || 0,
                status: stream.status,
                mediaSequence: currentSequence,
                segmentCount: segmentCount,
                errorCount: stream.health.totalErrors || 0
            });
        } catch (histErr) {
            console.error(`[METRICS] ${stream.name}: ${histErr.message}`);
        }

        // Save updated recent metrics to database
        try {
            await stream.save();
        } catch (saveErr) {
            if (saveErr.name !== 'VersionError') {
                console.error(`[METRICS SAVE] ${stream.name}: ${saveErr.message}`);
            }
        }

        io.emit('stream:update', stream);

        console.log(`[OK] ${stream.name}: seq=${currentSequence}, segments=${segmentCount}`);

    } catch (err) {
        console.error(`[FATAL] ${stream.name}:`, err.message);
        stream.status = 'error';
        addError(stream, ErrorTypes.MANIFEST_RETRIEVAL, err.message);

        try {
            await stream.save();
        } catch (saveErr) {
            // Handle VersionError specifically to avoid crash
            if (saveErr.name === 'VersionError') {
                console.warn(`[WARN] ${stream.name}: VersionError during error save - skipping update`);
                return;
            }
            console.error(`[FATAL] ${stream.name}: Failed to save error state:`, saveErr.message);
        }
        io.emit('stream:update', stream);
    }
}

async function monitorLoop(io) {
    const streams = await Stream.find();

    for (const stream of streams) {
        await checkStream(stream, io);
    }
}

module.exports = function (io) {
    console.log(`[MONITOR] Starting with ${MONITOR_INTERVAL}ms interval`);

    let isRunning = false;

    async function runMonitor() {
        if (isRunning) return;
        isRunning = true;

        try {
            await monitorLoop(io);
        } catch (err) {
            console.error('[MONITOR] Global loop error:', err);
        } finally {
            isRunning = false;
            // Schedule next run only after current one finishes
            setTimeout(runMonitor, MONITOR_INTERVAL);
        }
    }

    // Start immediately
    runMonitor();
};
