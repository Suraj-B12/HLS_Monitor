const mongoose = require('mongoose');

// Error types matching Eyevinn exactly
const ErrorTypes = {
    MANIFEST_RETRIEVAL: 'Manifest Retrieval',
    MEDIA_SEQUENCE: 'Media Sequence',
    PLAYLIST_SIZE: 'Playlist Size',
    PLAYLIST_CONTENT: 'Playlist Content',
    SEGMENT_CONTINUITY: 'Segment Continuity',
    DISCONTINUITY_SEQUENCE: 'Discontinuity Sequence',
    STALE_MANIFEST: 'Stale Manifest'
};

const StreamSchema = new mongoose.Schema({
    name: { type: String, required: true },
    url: { type: String, required: true, unique: true },
    status: {
        type: String,
        enum: ['online', 'offline', 'error', 'stale'],
        default: 'offline'
    },

    // --- EYEVINN HEALTH METRICS ---
    health: {
        isStale: { type: Boolean, default: false },
        lastManifestUpdate: { type: Date, default: null },
        timeSinceLastUpdate: { type: Number, default: 0 },
        staleThreshold: { type: Number, default: 7000 },

        mediaSequence: { type: Number, default: -1 },
        previousMediaSequence: { type: Number, default: -1 },
        sequenceJumps: { type: Number, default: 0 },
        sequenceResets: { type: Number, default: 0 },

        discontinuitySequence: { type: Number, default: 0 },
        discontinuityCount: { type: Number, default: 0 },

        segmentCount: { type: Number, default: 0 },
        targetDuration: { type: Number, default: 0 },
        playlistType: { type: String, default: 'LIVE' },

        totalErrors: { type: Number, default: 0 },
        timeSinceLastError: { type: Number, default: 0 },

        // --- SLIDING WINDOW METRICS (last 100 segments) ---
        recentErrors: { type: Number, default: 0 },
        recentSequenceJumps: { type: Number, default: 0 },
        recentSequenceResets: { type: Number, default: 0 },
        lastErrorTime: { type: Date, default: null }
    },

    // --- DEEP VIDEO/AUDIO STATS ---
    stats: {
        bandwidth: Number,
        resolution: String,
        fps: Number,
        video: {
            codec: String,
            profile: String,
            level: String,
            width: Number,
            height: Number,
            pixFmt: String,
            colorSpace: String,
            bitRate: Number
        },
        audio: {
            codec: String,
            channels: Number,
            sampleRate: Number,
            bitRate: Number,
            // --- ENHANCED AUDIO METRICS ---
            peakDb: Number,           // Peak decibel level
            avgDb: Number,            // Average decibel level
            channelLayout: String,    // Human-readable layout (Stereo, 5.1, etc.)
            isSilent: Boolean         // Silence detection flag
        },
        container: {
            formatName: String,
            duration: Number,
            size: Number,
            bitRate: Number
        }
    },

    // --- ERROR LOG ---
    // NOTE: No cap anymore - relying on 7-day TTL in MetricsHistory for cleanup
    // Errors older than 7 days are cleaned by MongoDB TTL index automatically
    streamErrors: [{
        eid: String,
        date: { type: Date, default: Date.now },
        errorType: { type: String, enum: Object.values(ErrorTypes) },
        mediaType: String,
        variant: String,
        details: String,
        code: Number
    }],

    thumbnail: String,
    lastChecked: { type: Date, default: Date.now }

}, { timestamps: true });

// Auto-cleanup: Remove errors older than 7 days on save
// Wrapped in try-catch to prevent cleanup failures from blocking saves
StreamSchema.pre('save', function () {
    try {
        if (this.streamErrors && Array.isArray(this.streamErrors) && this.streamErrors.length > 0) {
            const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
            this.streamErrors = this.streamErrors.filter(err => {
                // Keep errors that have a valid date within the last 7 days
                if (!err || !err.date) return false;
                try {
                    const errDate = new Date(err.date);
                    return !isNaN(errDate.getTime()) && errDate > sevenDaysAgo;
                } catch {
                    return false; // Remove malformed errors
                }
            });
        }
    } catch (err) {
        // If cleanup fails, log but don't block the save
        console.error('[STREAM] Error cleanup failed:', err.message);
    }
});

module.exports = mongoose.model('Stream', StreamSchema);
module.exports.ErrorTypes = ErrorTypes;
