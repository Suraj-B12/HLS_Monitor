import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { io } from 'socket.io-client';
import axios from 'axios';
import { ArrowLeft, Download, Activity, Zap, Volume2, Box, AlertTriangle, CheckCircle, Clock, RefreshCw, Radio, TrendingUp, Play } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Area, AreaChart } from 'recharts';

// Health Score Calculation - Based on status and recent SIGNIFICANT errors
function calculateHealthScore(stream) {
    let score = 100;
    const health = stream.health || {};

    // Status penalties (hard caps)
    if (stream.status === 'error') return 60;
    if (stream.status === 'offline') return 50;
    if (health.isStale) score -= 15;

    // Recent errors penalty - now only counts significant gaps (3+)
    const errors = health.recentErrors || 0;
    if (errors > 0) {
        score -= Math.min(errors * 3, 30);
    }

    return Math.max(0, Math.min(100, score));
}

function getHealthColor(score) {
    if (score >= 80) return { bg: 'bg-emerald-500', text: 'text-emerald-400', label: 'HEALTHY' };
    if (score >= 50) return { bg: 'bg-amber-500', text: 'text-amber-400', label: 'WARNING' };
    return { bg: 'bg-rose-500', text: 'text-rose-400', label: 'CRITICAL' };
}

// Format dB level for display - converts negative dB to user-friendly format
// Audio dB values are typically negative (0 dB = max, -∞ = silence)
// We display as absolute value with "dBFS" (decibels Full Scale) suffix
function formatDbLevel(dbValue) {
    if (dbValue == null || !isFinite(dbValue)) return '-';
    // Convert to absolute value for cleaner display
    const absValue = Math.abs(dbValue).toFixed(1);
    return `${absValue} dBFS`;
}

// Signal Strength Indicator
const SignalMeter = ({ level, label, rawValue }) => {
    const getBarColor = () => {
        if (level >= 70) return '#10b981';
        if (level >= 40) return '#f59e0b';
        return '#ef4444';
    };

    const bars = 15;
    const activeCount = Math.floor((level / 100) * bars);

    return (
        <div className="bg-black/40 rounded-xl p-4 border border-white/10">
            <div className="flex justify-between items-center mb-3">
                <span className="text-xs font-bold uppercase text-white/60">{label}</span>
                <span className={`text-lg font-mono font-bold ${level >= 70 ? 'text-emerald-400' : level >= 40 ? 'text-amber-400' : 'text-rose-400'}`}>
                    {rawValue || `${level.toFixed(0)}%`}
                </span>
            </div>
            <div className="flex gap-1 h-8">
                {Array.from({ length: bars }).map((_, i) => (
                    <div
                        key={i}
                        className="flex-1 rounded transition-all duration-150"
                        style={{
                            backgroundColor: i < activeCount ? getBarColor() : '#1f2937',
                            boxShadow: i < activeCount ? `0 0 6px ${getBarColor()}` : 'none'
                        }}
                    />
                ))}
            </div>
            <div className="flex justify-between text-[10px] text-white/30 mt-1">
                <span>WEAK</span>
                <span>STRONG</span>
            </div>
        </div>
    );
};

// Custom Tooltip
const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
        return (
            <div className="bg-surface border border-white/20 rounded-lg p-3 shadow-xl">
                <p className="text-white/60 text-xs mb-2">{label}</p>
                {payload.map((entry, index) => (
                    <p key={index} className="text-sm font-mono" style={{ color: entry.color }}>
                        {entry.name}: <span className="font-bold">{entry.value?.toFixed(2)}</span>
                        {entry.name.includes('Bitrate') ? ' Mbps' : '%'}
                    </p>
                ))}
            </div>
        );
    }
    return null;
};

// Lazy Loading Errors Panel with Infinite Scroll
const ErrorsPanel = ({ streamId }) => {
    const [errors, setErrors] = useState([]);
    const [loading, setLoading] = useState(false);
    const [page, setPage] = useState(1);
    const [hasMore, setHasMore] = useState(true);
    const [total, setTotal] = useState(0);
    const containerRef = React.useRef(null);

    const loadErrors = async (pageNum) => {
        if (loading) return;
        setLoading(true);
        try {
            const res = await axios.get(`/api/streams/${streamId}/errors?page=${pageNum}&limit=20`);
            if (pageNum === 1) {
                setErrors(res.data.errors);
            } else {
                setErrors(prev => [...prev, ...res.data.errors]);
            }
            setTotal(res.data.total);
            setHasMore(res.data.hasMore);
            setPage(pageNum);
        } catch (err) {
            console.error('Error loading errors:', err);
        }
        setLoading(false);
    };

    useEffect(() => {
        loadErrors(1);
    }, [streamId]);

    const handleScroll = (e) => {
        const { scrollTop, scrollHeight, clientHeight } = e.target;
        if (scrollHeight - scrollTop <= clientHeight + 50 && hasMore && !loading) {
            loadErrors(page + 1);
        }
    };

    return (
        <div className="glass-panel p-6 mb-8">
            <h3 className="text-sm font-bold text-rose-400 uppercase tracking-wider mb-4 flex items-center gap-2">
                <AlertTriangle size={14} /> Errors ({total})
            </h3>
            {errors.length === 0 && !loading ? (
                <div className="text-white/30 text-center py-8 flex items-center justify-center gap-2">
                    <CheckCircle size={18} className="text-emerald-400" /> No errors recorded
                </div>
            ) : (
                <div
                    ref={containerRef}
                    onScroll={handleScroll}
                    className="space-y-2 max-h-64 overflow-y-auto pr-2"
                    style={{ scrollbarWidth: 'thin' }}
                >
                    {errors.map((err, i) => (
                        <div key={`${err.eid || i}-${err.date}`} className="bg-rose-500/10 border border-rose-500/20 rounded-lg p-3 font-mono text-xs">
                            <div className="flex justify-between mb-1">
                                <span className="text-rose-300 font-bold">{err.errorType}</span>
                                <span className="text-white/40">{err.date ? new Date(err.date).toLocaleTimeString() : '-'}</span>
                            </div>
                            <div className="text-white/60">{err.details}</div>
                        </div>
                    ))}
                    {loading && (
                        <div className="text-center py-2 text-white/40">
                            <RefreshCw size={14} className="inline animate-spin mr-2" /> Loading more...
                        </div>
                    )}
                    {!hasMore && errors.length > 0 && (
                        <div className="text-center py-2 text-white/30 text-xs">— End of errors —</div>
                    )}
                </div>
            )}
        </div>
    );
};

const StreamDetail = () => {
    const { id } = useParams();
    const [stream, setStream] = useState(null);
    const [loading, setLoading] = useState(true);
    const [signalHistory, setSignalHistory] = useState([]);
    const [liveStats, setLiveStats] = useState({ videoLevel: 0, audioLevel: 0, fps: 0, videoBitrate: 0, audioBitrate: 0 });

    // Log Date Selection
    const [isDateModalOpen, setIsDateModalOpen] = useState(false);
    const [availableDates, setAvailableDates] = useState([]);
    const [loadingDates, setLoadingDates] = useState(false);

    useEffect(() => {
        axios.get(`/api/streams/${id}`)
            .then(res => { setStream(res.data); setLoading(false); })
            .catch(err => { console.error(err); setLoading(false); });

        const loadHistory = () => {
            axios.get(`/api/streams/${id}/metrics`)
                .then(res => {
                    const formatted = res.data.map(m => ({
                        time: new Date(m.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
                        videoLevel: m.videoLevel || 0,
                        audioLevel: m.audioLevel || 0,
                        videoBitrate: m.videoBitrate ? (m.videoBitrate / 1000000) : 0,
                        audioBitrate: m.audioBitrate ? (m.audioBitrate / 1000) : 0,
                    }));
                    setSignalHistory(formatted);
                    if (formatted.length > 0) {
                        const latest = res.data[res.data.length - 1];
                        setLiveStats({
                            videoLevel: formatted[formatted.length - 1].videoLevel,
                            audioLevel: formatted[formatted.length - 1].audioLevel,
                            videoBitrate: latest.videoBitrate || 0,
                            audioBitrate: latest.audioBitrate || 0,
                            fps: latest.fps || 0
                        });
                    }
                })
                .catch(err => console.error(err));
        };

        loadHistory();

        const socket = io();
        socket.on('stream:signal', (data) => {
            if (data.id === id) {
                setLiveStats({
                    videoLevel: data.video,
                    audioLevel: data.audio,
                    videoBitrate: data.videoBitrate,
                    audioBitrate: data.audioBitrate,
                    fps: data.fps
                });
                setSignalHistory(prev => [...prev, {
                    time: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
                    videoLevel: data.video,
                    audioLevel: data.audio,
                    videoBitrate: data.videoBitrate / 1000000,
                    audioBitrate: data.audioBitrate / 1000,
                }]);
            }
        });
        socket.on('stream:update', (updated) => { if (updated._id === id) setStream(updated); });
        socket.on('stream:sprite', (data) => { if (data.id === id) setStream(prev => prev ? { ...prev, thumbnail: data.url } : prev); });

        const historyInterval = setInterval(loadHistory, 30000);
        return () => { socket.disconnect(); clearInterval(historyInterval); };
    }, [id]);

    const downloadLog = () => window.open(`/api/streams/${id}/log`, '_blank');

    if (loading) return <div className="min-h-screen bg-surface flex items-center justify-center"><RefreshCw className="w-8 h-8 text-primary animate-spin" /></div>;
    if (!stream) return <div className="min-h-screen bg-surface flex items-center justify-center text-white/50">Stream not found</div>;

    const healthScore = calculateHealthScore(stream);
    const healthColor = getHealthColor(healthScore);
    const health = stream.health || {};
    const stats = stream.stats || {};

    // Calculate chart width - 8px per data point, minimum 800px
    const chartWidth = Math.max(800, signalHistory.length * 8);

    return (
        <div className="min-h-screen bg-surface text-white p-6">
            <div className="max-w-6xl mx-auto">
                <Link to="/" className="inline-flex items-center gap-2 text-white/50 hover:text-white mb-6 transition-colors">
                    <ArrowLeft size={18} /> Back to Dashboard
                </Link>

                {/* Header */}
                <div className="flex flex-col lg:flex-row gap-6 mb-8">
                    <div className="lg:w-1/3">
                        <div className="aspect-video bg-black/60 rounded-xl overflow-hidden border border-white/10">
                            {stream.thumbnail ? <img src={stream.thumbnail} alt="Live" className="w-full h-full object-cover" /> : <div className="w-full h-full flex items-center justify-center text-white/20">No Signal</div>}
                        </div>
                    </div>
                    <div className="lg:w-2/3 flex flex-col justify-center">
                        <h1 className="text-3xl font-bold mb-2">{stream.name}</h1>
                        <p className="text-white/40 font-mono text-sm mb-4 break-all">{stream.url}</p>
                        <div className="flex items-center gap-4">
                            <div className={`w-24 h-24 rounded-full ${healthColor.bg} flex items-center justify-center`}>
                                <span className="text-3xl font-bold text-white">{healthScore}</span>
                            </div>
                            <div>
                                <div className={`text-xl font-bold ${healthColor.text}`}>{healthColor.label}</div>
                                <div className="text-white/50 text-sm">Overall Health Score</div>
                            </div>
                        </div>
                    </div>
                </div>

                <div className="flex flex-wrap gap-4">
                    <button
                        onClick={async () => {
                            setLoadingDates(true);
                            try {
                                const res = await axios.get(`/api/streams/${id}/logs/dates`);
                                if (res.data && res.data.length > 0) {
                                    setAvailableDates(res.data);
                                    setIsDateModalOpen(true);
                                } else {
                                    alert("No historical logs available.");
                                }
                            } catch (err) {
                                console.error(err);
                                alert("Failed to fetch log dates.");
                            }
                            setLoadingDates(false);
                        }}
                        disabled={loadingDates}
                        className="mb-8 px-6 py-3 bg-primary/20 hover:bg-primary/30 border border-primary/50 rounded-lg text-primary font-bold flex items-center gap-2 transition-colors disabled:opacity-50"
                    >
                        {loadingDates ? <RefreshCw className="animate-spin" size={18} /> : <Download size={18} />}
                        Download Daily Log
                    </button>

                    {/* Play Stream Button - Opens HLS player in new tab (client-side only) */}
                    <button
                        onClick={() => {
                            // Create a standalone HTML page with HLS.js player
                            const playerHtml = `
<!DOCTYPE html>
<html>
<head>
    <title>${stream.name} - HLS Player</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0a0f; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
        video { max-width: 100%; max-height: 100vh; background: #000; }
        .error { color: #f87171; font-family: system-ui; text-align: center; padding: 2rem; }
    </style>
</head>
<body>
    <video id="video" controls autoplay></video>
    <script>
        const video = document.getElementById('video');
        const url = '${stream.url}';
        if (Hls.isSupported()) {
            const hls = new Hls({
                liveSyncDurationCount: 3,      // Start 3 segments from live edge
                liveMaxLatencyDurationCount: 5 // Max 5 segments behind
            });
            hls.loadSource(url);
            hls.attachMedia(video);
            // Seek to live edge when manifest is loaded
            hls.on(Hls.Events.MANIFEST_PARSED, () => {
                video.currentTime = video.duration || 0;
                video.play();
            });
            hls.on(Hls.Events.ERROR, (e, data) => {
                if (data.fatal) document.body.innerHTML = '<div class="error">Failed to load stream: ' + data.details + '</div>';
            });
        } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
            video.src = url;
        } else {
            document.body.innerHTML = '<div class="error">HLS not supported in this browser</div>';
        }
    </script>
</body>
</html>`;
                            const blob = new Blob([playerHtml], { type: 'text/html' });
                            const playerUrl = URL.createObjectURL(blob);
                            window.open(playerUrl, '_blank');
                        }}
                        className="mb-8 px-6 py-3 bg-emerald-500/20 hover:bg-emerald-500/30 border border-emerald-500/50 rounded-lg text-emerald-400 font-bold flex items-center gap-2 transition-colors"
                    >
                        <Play size={18} />
                        Play Stream
                    </button>
                </div>

                {/* Date Selection Modal */}
                {isDateModalOpen && (
                    <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4" onClick={() => setIsDateModalOpen(false)}>
                        <div className="bg-slate-900/95 border border-white/10 rounded-2xl w-full max-w-sm p-6 shadow-2xl" onClick={e => e.stopPropagation()}>
                            <div className="flex items-center gap-3 mb-6">
                                <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center">
                                    <Clock className="text-primary" size={20} />
                                </div>
                                <div>
                                    <h2 className="text-lg font-bold text-white">Download Log</h2>
                                    <p className="text-white/40 text-xs">Select a date</p>
                                </div>
                            </div>

                            <div className="space-y-2 max-h-80 overflow-y-auto">
                                {availableDates.map((item, index) => {
                                    const dateStr = item.date || item;
                                    const dateObj = new Date(dateStr + 'T00:00:00');
                                    const dayName = dateObj.toLocaleDateString('en-US', { weekday: 'short' });
                                    const monthDay = dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                                    const hasErrors = item.errorCount > 0;
                                    const isToday = index === 0;

                                    return (
                                        <button
                                            key={dateStr}
                                            onClick={() => {
                                                window.open(`/api/streams/${id}/log?date=${dateStr}`, '_blank');
                                                setIsDateModalOpen(false);
                                            }}
                                            className={`w-full flex items-center justify-between px-4 py-3 rounded-xl transition-all ${hasErrors
                                                    ? 'bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/20'
                                                    : 'bg-white/5 hover:bg-white/10 border border-white/5'
                                                }`}
                                        >
                                            <div className="flex items-center gap-3">
                                                <div className={`w-2 h-2 rounded-full ${hasErrors ? 'bg-rose-400' : 'bg-emerald-400'}`} />
                                                <div className="text-left">
                                                    <div className="font-medium text-white">
                                                        {monthDay}
                                                        {isToday && <span className="ml-2 text-xs text-primary">(Today)</span>}
                                                    </div>
                                                    <div className="text-xs text-white/40">{dayName}</div>
                                                </div>
                                            </div>
                                            <span className={`text-sm font-medium ${hasErrors ? 'text-rose-400' : 'text-emerald-400'}`}>
                                                {hasErrors ? `${item.errorCount} errors` : 'Healthy'}
                                            </span>
                                        </button>
                                    );
                                })}
                            </div>

                            <button
                                onClick={() => setIsDateModalOpen(false)}
                                className="w-full mt-4 py-2.5 text-white/50 hover:text-white text-sm font-medium rounded-lg hover:bg-white/5 transition-colors"
                            >
                                Cancel
                            </button>
                        </div>
                    </div>
                )}

                {/* LIVE SIGNAL METERS */}
                <div className="glass-panel p-6 mb-8">
                    <h3 className="text-lg font-bold mb-4 flex items-center gap-2">
                        <Radio size={18} className="text-emerald-400" /> Live Signal Strength
                        <span className="ml-2 px-2 py-0.5 bg-emerald-500/20 text-emerald-400 text-xs rounded-full animate-pulse">● LIVE</span>
                    </h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
                        <SignalMeter level={liveStats.videoLevel} label="Video Signal" rawValue={liveStats.videoBitrate ? `${(liveStats.videoBitrate / 1000000).toFixed(2)} Mbps` : null} />
                        <SignalMeter level={liveStats.audioLevel} label="Audio Signal" rawValue={liveStats.audioBitrate ? `${(liveStats.audioBitrate / 1000).toFixed(0)} kbps` : null} />
                        <div className="bg-black/40 rounded-xl p-4 border border-white/10">
                            <div className="text-xs font-bold uppercase text-white/60 mb-1">Frame Rate</div>
                            <div className="text-2xl font-mono font-bold text-cyan-400">{liveStats.fps?.toFixed(2) || '--'} <span className="text-sm">fps</span></div>
                        </div>
                        <div className="bg-black/40 rounded-xl p-4 border border-white/10">
                            <div className="text-xs font-bold uppercase text-white/60 mb-1">Health Score</div>
                            <div className={`text-2xl font-mono font-bold ${healthColor.text}`}>{healthScore}<span className="text-sm">/100</span></div>
                        </div>
                    </div>
                    <div className="flex justify-center gap-6 text-xs">
                        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-emerald-500"></span> Strong (70%+)</div>
                        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-amber-500"></span> Medium (40-70%)</div>
                        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-rose-500"></span> Weak (&lt;40%)</div>
                    </div>
                </div>

                {/* SCROLLABLE SIGNAL HISTORY GRAPHS */}
                <div className="glass-panel p-6 mb-8">
                    <h3 className="text-lg font-bold mb-2 flex items-center gap-2">
                        <TrendingUp size={18} className="text-primary" />
                        Signal History ({signalHistory.length} samples since start)
                    </h3>
                    <p className="text-white/40 text-sm mb-4">
                        ← Scroll horizontally to see all data from the start. Data collected every 7 seconds. →
                    </p>

                    {signalHistory.length > 1 ? (
                        <div className="space-y-6">
                            {/* Video Bitrate Chart */}
                            <div>
                                <h4 className="text-sm font-bold text-purple-400 mb-3">📊 Video Bitrate (Mbps)</h4>
                                <div className="overflow-x-auto rounded-lg border border-white/10 bg-black/20" style={{ scrollbarWidth: 'thin' }}>
                                    <div style={{ width: chartWidth, height: 180, padding: '10px 0' }}>
                                        <AreaChart width={chartWidth} height={160} data={signalHistory} margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
                                            <defs>
                                                <linearGradient id="videoGrad" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.5} />
                                                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                                            <XAxis dataKey="time" stroke="#666" tick={{ fill: '#888', fontSize: 9 }} interval={Math.max(1, Math.floor(signalHistory.length / 20))} />
                                            <YAxis stroke="#666" tick={{ fill: '#888', fontSize: 10 }} />
                                            <Tooltip content={<CustomTooltip />} />
                                            <Area type="monotone" dataKey="videoBitrate" stroke="#8b5cf6" fill="url(#videoGrad)" strokeWidth={2} name="Video Bitrate" />
                                        </AreaChart>
                                    </div>
                                </div>
                            </div>

                            {/* Signal Strength Chart */}
                            <div>
                                <h4 className="text-sm font-bold text-cyan-400 mb-3">📈 Signal Strength (%)</h4>
                                <div className="overflow-x-auto rounded-lg border border-white/10 bg-black/20" style={{ scrollbarWidth: 'thin' }}>
                                    <div style={{ width: chartWidth, height: 180, padding: '10px 0' }}>
                                        <LineChart width={chartWidth} height={160} data={signalHistory} margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
                                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                                            <XAxis dataKey="time" stroke="#666" tick={{ fill: '#888', fontSize: 9 }} interval={Math.max(1, Math.floor(signalHistory.length / 20))} />
                                            <YAxis domain={[0, 100]} stroke="#666" tick={{ fill: '#888', fontSize: 10 }} />
                                            <Tooltip content={<CustomTooltip />} />
                                            <Legend />
                                            <Line type="monotone" dataKey="videoLevel" stroke="#8b5cf6" strokeWidth={2} dot={false} name="Video %" />
                                            <Line type="monotone" dataKey="audioLevel" stroke="#06b6d4" strokeWidth={2} dot={false} name="Audio %" />
                                        </LineChart>
                                    </div>
                                </div>
                                <div className="flex justify-between text-xs text-white/40 mt-2 px-2">
                                    <span>← Oldest</span>
                                    <span>Live →</span>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div className="h-48 flex items-center justify-center text-white/30">
                            <div className="text-center">
                                <RefreshCw className="w-8 h-8 mx-auto mb-2 animate-spin" />
                                <p>Loading historical data...</p>
                            </div>
                        </div>
                    )}
                </div>

                {/* Stats Grid */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
                    <div className="glass-panel p-4">
                        <h3 className="text-xs font-bold text-amber-400 uppercase tracking-wider mb-3 flex items-center gap-1"><Activity size={12} /> Health</h3>
                        <div className="space-y-2 text-sm font-mono">
                            <div className="flex justify-between"><span className="text-white/50">Status</span><span className={stream.status === 'online' ? 'text-emerald-400' : 'text-rose-400'}>{stream.status?.toUpperCase()}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Stale</span><span className={health.isStale ? 'text-amber-400' : 'text-emerald-400'}>{health.isStale ? 'YES' : 'NO'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Media Seq</span><span className="text-white">{health.mediaSequence ?? '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Errors</span><span className={health.totalErrors > 0 ? 'text-rose-400' : 'text-emerald-400'}>{health.totalErrors ?? 0}</span></div>
                        </div>
                    </div>
                    <div className="glass-panel p-4">
                        <h3 className="text-xs font-bold text-primary uppercase tracking-wider mb-3 flex items-center gap-1"><Zap size={12} /> Video</h3>
                        <div className="space-y-2 text-sm font-mono">
                            <div className="flex justify-between"><span className="text-white/50">Codec</span><span className="text-white">{stats.video?.codec || '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Resolution</span><span className="text-white">{stats.resolution || '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">FPS</span><span className="text-white">{stats.fps?.toFixed(2) || '-'}</span></div>
                        </div>
                    </div>
                    <div className="glass-panel p-4">
                        <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-3 flex items-center gap-1"><Volume2 size={12} /> Audio</h3>
                        <div className="space-y-2 text-sm font-mono">
                            <div className="flex justify-between"><span className="text-white/50">Codec</span><span className="text-white">{stats.audio?.codec || '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Channels</span><span className="text-white">{stats.audio?.channelLayout || stats.audio?.channels || '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Sample Rate</span><span className="text-white">{stats.audio?.sampleRate ? `${stats.audio.sampleRate}Hz` : '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Peak Level</span><span className={stats.audio?.isSilent ? 'text-amber-400' : 'text-white'}>{formatDbLevel(stats.audio?.peakDb)}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Avg Level</span><span className="text-white">{formatDbLevel(stats.audio?.avgDb)}</span></div>
                            {stats.audio?.isSilent && (
                                <div className="mt-2 px-2 py-1 bg-amber-500/20 border border-amber-500/30 rounded text-amber-400 text-xs flex items-center gap-1">
                                    <AlertTriangle size={12} /> Possible silence detected
                                </div>
                            )}
                        </div>
                    </div>
                    <div className="glass-panel p-4">
                        <h3 className="text-xs font-bold text-emerald-400 uppercase tracking-wider mb-3 flex items-center gap-1"><Box size={12} /> Container</h3>
                        <div className="space-y-2 text-sm font-mono">
                            <div className="flex justify-between"><span className="text-white/50">Format</span><span className="text-white">{stats.container?.formatName || '-'}</span></div>
                            <div className="flex justify-between"><span className="text-white/50">Bitrate</span><span className="text-white">{stats.container?.bitRate ? `${(stats.container.bitRate / 1000).toFixed(0)}kbps` : '-'}</span></div>
                        </div>
                    </div>
                </div>

                {/* Errors - Lazy Loading */}
                <ErrorsPanel streamId={id} />

                <div className="text-center text-white/30 text-xs font-mono"><Clock size={12} className="inline mr-1" /> Last Checked: {stream.lastChecked ? new Date(stream.lastChecked).toLocaleString() : '-'}</div>
            </div>
        </div>
    );
};

export default StreamDetail;
