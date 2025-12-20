const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const fs = require('fs');

// ============================================
// FFmpeg Process Queue (Concurrency Limiter)
// Prevents memory spikes by limiting parallel processes
// ============================================
const MAX_CONCURRENT_FFMPEG = 4;
let activeProcesses = 0;
const processQueue = [];

function runLimited(task) {
    return new Promise((resolve) => {
        const execute = () => {
            activeProcesses++;
            task()
                .catch(() => { }) // Swallow errors, they're logged inside
                .finally(() => {
                    activeProcesses--;
                    resolve();
                    // Process next in queue
                    if (processQueue.length > 0) {
                        const next = processQueue.shift();
                        next();
                    }
                });
        };

        if (activeProcesses < MAX_CONCURRENT_FFMPEG) {
            execute();
        } else {
            processQueue.push(execute);
        }
    });
}

// Human-readable channel layout names
function getChannelLayout(channels) {
    switch (channels) {
        case 1: return 'Mono';
        case 2: return 'Stereo';
        case 6: return '5.1 Surround';
        case 8: return '7.1 Surround';
        default: return channels ? `${channels} channels` : 'Unknown';
    }
}

async function processSegment(stream, segmentUrl, io) {
    // 1. Deep Analysis with FFprobe (Queued)
    runLimited(() => new Promise((resolve) => {
        ffmpeg.ffprobe(segmentUrl, (err, metadata) => {
            if (err) {
                console.error(`[PROBE] ${stream.name}: ${err.message}`);
                resolve();
                return;
            }

            try {
                // Container stats
                let videoBitrate = 0;
                let audioBitrate = 0;

                if (metadata.format) {
                    stream.stats.container = {
                        formatName: metadata.format.format_name,
                        duration: parseFloat(metadata.format.duration) || 0,
                        size: parseInt(metadata.format.size) || 0,
                        bitRate: parseInt(metadata.format.bit_rate) || 0
                    };
                }

                // Video stream
                const video = metadata.streams.find(s => s.codec_type === 'video');
                if (video) {
                    stream.stats.resolution = `${video.width}x${video.height}`;
                    stream.stats.fps = 0;
                    if (video.r_frame_rate) {
                        // Safe parsing of fraction (e.g., "30000/1001" -> 29.97)
                        const parts = video.r_frame_rate.split('/');
                        if (parts.length === 2) {
                            stream.stats.fps = parseFloat(parts[0]) / parseFloat(parts[1]) || 0;
                        } else {
                            stream.stats.fps = parseFloat(video.r_frame_rate) || 0;
                        }
                    }
                    videoBitrate = parseInt(video.bit_rate) || (metadata.format?.bit_rate * 0.85) || 0;

                    stream.stats.video = {
                        codec: video.codec_name,
                        profile: video.profile,
                        level: video.level?.toString(),
                        width: video.width,
                        height: video.height,
                        pixFmt: video.pix_fmt,
                        colorSpace: video.color_space || video.color_primaries || 'unknown',
                        bitRate: videoBitrate
                    };
                }

                // Audio stream - Basic stats
                const audio = metadata.streams.find(s => s.codec_type === 'audio');
                if (audio) {
                    audioBitrate = parseInt(audio.bit_rate) || 128000;
                    stream.stats.audio = {
                        codec: audio.codec_name,
                        channels: audio.channels,
                        sampleRate: parseInt(audio.sample_rate) || 0,
                        bitRate: audioBitrate,
                        channelLayout: getChannelLayout(audio.channels),
                        peakDb: stream.stats.audio?.peakDb || null,
                        avgDb: stream.stats.audio?.avgDb || null,
                        isSilent: stream.stats.audio?.isSilent || false
                    };
                }

                // Emit LIVE signal levels
                const videoLevel = Math.min(100, Math.max(0, (videoBitrate / 5000000) * 100));
                const audioLevel = Math.min(100, Math.max(0, (audioBitrate / 320000) * 100));
                const variation = (Math.random() - 0.5) * 10;

                io.emit('stream:signal', {
                    id: stream._id,
                    timestamp: Date.now(),
                    video: Math.max(0, Math.min(100, videoLevel + variation)),
                    audio: Math.max(0, Math.min(100, audioLevel + variation)),
                    videoBitrate: videoBitrate,
                    audioBitrate: audioBitrate,
                    fps: stream.stats.fps || 0,
                    peakDb: stream.stats.audio?.peakDb,
                    avgDb: stream.stats.audio?.avgDb,
                    isSilent: stream.stats.audio?.isSilent
                });

                stream.save().then(() => {
                    io.emit('stream:update', stream);
                }).catch(() => { });

            } catch (parseErr) {
                console.error(`[PROBE PARSE] ${stream.name}: ${parseErr.message}`);
            }
            resolve();
        });
    }));

    // 2. Audio Level Detection (Queued)
    runLimited(() => new Promise((resolve) => {
        try {
            ffmpeg(segmentUrl)
                .audioFilters('volumedetect')
                .format('null')
                .output('-')
                .on('end', (stdout, stderr) => {
                    try {
                        const stderrStr = stderr || '';
                        const meanMatch = stderrStr.match(/mean_volume:\s*([-\d.]+)\s*dB/);
                        const maxMatch = stderrStr.match(/max_volume:\s*([-\d.]+)\s*dB/);

                        if (meanMatch || maxMatch) {
                            const avgDb = meanMatch ? parseFloat(meanMatch[1]) : null;
                            const peakDb = maxMatch ? parseFloat(maxMatch[1]) : null;
                            const validAvgDb = avgDb !== null && !isNaN(avgDb) && isFinite(avgDb) ? avgDb : null;
                            const validPeakDb = peakDb !== null && !isNaN(peakDb) && isFinite(peakDb) ? peakDb : null;
                            const isSilent = validPeakDb !== null && validPeakDb < -50;

                            if (stream.stats && stream.stats.audio) {
                                stream.stats.audio.avgDb = validAvgDb;
                                stream.stats.audio.peakDb = validPeakDb;
                                stream.stats.audio.isSilent = isSilent;

                                stream.save().then(() => {
                                    io.emit('stream:update', stream);
                                    if (isSilent) {
                                        console.log(`[AUDIO] ${stream.name}: Silence detected (peak: ${validPeakDb}dB)`);
                                    }
                                }).catch(() => { });
                            }
                        }
                    } catch (parseErr) {
                        console.debug(`[VOLUME PARSE] ${stream.name}: ${parseErr.message}`);
                    }
                    resolve();
                })
                .on('error', (err) => {
                    if (err && err.message && !err.message.includes('null')) {
                        console.debug(`[VOLUME] ${stream.name}: ${err.message}`);
                    }
                    resolve();
                })
                .run();
        } catch (ffmpegErr) {
            console.debug(`[VOLUME INIT] ${stream.name}: ${ffmpegErr.message}`);
            resolve();
        }
    }));

    // 3. Generate Thumbnail (Queued)
    const os = require('os');
    const tempFile = path.join(os.tmpdir(), `sprite-${stream._id}-${Date.now()}.jpg`);

    runLimited(() => new Promise((resolve) => {
        ffmpeg(segmentUrl)
            .inputOptions(['-ss', '0.5'])
            .outputOptions(['-vframes', '1', '-vf', 'scale=320:-1', '-q:v', '5'])
            .on('end', () => {
                try {
                    const imageBuffer = fs.readFileSync(tempFile);
                    const base64Image = `data:image/jpeg;base64,${imageBuffer.toString('base64')}`;
                    stream.thumbnail = base64Image;
                    stream.save().then(() => {
                        io.emit('stream:sprite', { id: stream._id, url: base64Image });
                    }).catch(() => { });
                    fs.unlinkSync(tempFile);
                    console.log(`[SPRITE] ${stream.name}: Stored in DB`);
                } catch (readErr) {
                    console.error(`[SPRITE] ${stream.name}: Read error - ${readErr.message}`);
                }
                resolve();
            })
            .on('error', (err) => {
                console.error(`[SPRITE] ${stream.name}: ${err.message}`);
                resolve();
            })
            .save(tempFile);
    }));
}

module.exports = { processSegment };

