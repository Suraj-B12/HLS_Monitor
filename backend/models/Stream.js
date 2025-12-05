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
        timeSinceLastError: { type: Number, default: 0 }
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
            bitRate: Number
        },
        container: {
            formatName: String,
            duration: Number,
            size: Number,
            bitRate: Number
        }
    },

    // --- ERROR LOG ---
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

// Limit errors to 10
StreamSchema.pre('save', function () {
    if (this.streamErrors && this.streamErrors.length > 250) {
        this.streamErrors = this.streamErrors.slice(-250);
    }
});

module.exports = mongoose.model('Stream', StreamSchema);
module.exports.ErrorTypes = ErrorTypes;
