const mongoose = require('mongoose');

const AuditLogSchema = new mongoose.Schema({
    action: {
        type: String,
        enum: ['STREAM_ADDED', 'STREAM_DELETED', 'LOG_DOWNLOADED'],
        required: true
    },
    streamId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Stream'
    },
    streamName: String,
    streamUrl: String,
    details: String,
    ipAddress: String,
    userAgent: String,
    deviceName: String,
    timestamp: {
        type: Date,
        default: Date.now
    }
}, { timestamps: true });

// Index for efficient querying
AuditLogSchema.index({ timestamp: -1 });
AuditLogSchema.index({ action: 1 });

module.exports = mongoose.model('AuditLog', AuditLogSchema);
