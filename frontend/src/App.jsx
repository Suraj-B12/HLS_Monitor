import React, { useState, useEffect, useRef } from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import axios from 'axios';
import { io } from 'socket.io-client';
import { Plus, Activity, LayoutDashboard, Search, Bell, BellOff, Volume2, VolumeX } from 'lucide-react';
import StreamCard from './components/StreamCard';
import StreamDetail from './components/StreamDetail';
import { audioSynth } from './utils/AudioSynth';

const socket = io('/', { path: '/socket.io' });

// Health Helper - Based on status and recent SIGNIFICANT errors
function calculateHealth(stream) {
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

function Dashboard() {
    const [streams, setStreams] = useState([]);
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [newStream, setNewStream] = useState({ name: '', url: '' });
    const [loading, setLoading] = useState(true);

    // Audio State
    const [activeAlarms, setActiveAlarms] = useState(new Set());
    const [acknowledged, setAcknowledged] = useState(new Set());
    const [isGlobalMute, setIsGlobalMute] = useState(false);
    const audioInitialized = useRef(false);

    // Initialize Audio Context on first interaction
    useEffect(() => {
        const initAudio = () => {
            if (!audioInitialized.current) {
                audioSynth.init();
                audioInitialized.current = true;
                window.removeEventListener('click', initAudio);
                console.log('Audio Context Initialized');
            }
        };
        window.addEventListener('click', initAudio);
        return () => window.removeEventListener('click', initAudio);
    }, []);

    // Check for Alarms
    useEffect(() => {
        if (loading) return;

        const newAlarms = new Set();
        let hasCritical = false;
        let hasWarning = false;

        streams.forEach(s => {
            if (acknowledged.has(s._id)) return;

            const score = calculateHealth(s);
            const isCritical = score < 40 || s.status === 'offline';
            const isWarning = s.status === 'error' && !isCritical;

            if (isCritical) {
                newAlarms.add(s._id);
                hasCritical = true;
            } else if (isWarning) {
                newAlarms.add(s._id);
                hasWarning = true;
            }
        });

        setActiveAlarms(newAlarms);

        if (isGlobalMute) {
            audioSynth.stopAll();
        } else {
            if (hasCritical) {
                audioSynth.startSiren();
            } else if (hasWarning) {
                audioSynth.startAlarm(); // Beep
            } else {
                audioSynth.stopAll(); // Silence
            }
        }

    }, [streams, acknowledged, loading, isGlobalMute]);

    const handleAcknowledge = (id) => {
        setAcknowledged(prev => new Set(prev).add(id));
        // If clicking on a card, we stop the specific alarm for that stream logic is handled by effect re-run
    };

    const toggleMute = () => {
        const muted = audioSynth.toggleMute();
        setIsGlobalMute(muted);
    };

    useEffect(() => {
        fetchStreams();

        socket.on('connect', () => {
            console.log('Connected to backend');
        });

        socket.on('stream:update', (updatedStream) => {
            setStreams(prev => prev.map(s => s._id === updatedStream._id ? updatedStream : s));
        });

        socket.on('stream:update', (updatedStream) => {
            setStreams(prev => prev.map(s => s._id === updatedStream._id ? updatedStream : s));
        });

        socket.on('stream:sprite', ({ id, url }) => {
            setStreams(prev => prev.map(s => s._id === id ? { ...s, thumbnail: url } : s));
        });

        socket.on('stream:added', (newStream) => {
            console.log('Socket: Stream Added', newStream);
            setStreams(prev => {
                if (prev.find(s => s._id === newStream._id)) return prev;
                return [newStream, ...prev];
            });
        });

        socket.on('stream:deleted', (deletedId) => {
            console.log('Socket: Stream Deleted', deletedId);
            setStreams(prev => prev.filter(s => s._id !== deletedId));
        });

        return () => {
            socket.off('stream:update');
            socket.off('stream:sprite');
            socket.off('stream:added');
            socket.off('stream:deleted');
        };
    }, []);

    const fetchStreams = async () => {
        try {
            const res = await axios.get('/api/streams');
            setStreams(res.data);
            setLoading(false);
        } catch (err) {
            console.error(err);
            setLoading(false);
        }
    };

    const handleAddStream = async (e) => {
        e.preventDefault();
        try {
            const res = await axios.post('/api/streams', newStream);
            setStreams([res.data, ...streams]);
            setIsModalOpen(false);
            setNewStream({ name: '', url: '' });
        } catch (err) {
            alert('Error adding stream');
        }
    };

    const handleDelete = async (id) => {
        const confirmation = prompt("To confirm deletion, please type: CONFIRM DELETE STREAM");

        if (confirmation !== 'CONFIRM DELETE STREAM') {
            if (confirmation !== null) { // Don't alert if user just cancelled
                alert('Deletion cancelled: Incorrect confirmation phrase.');
            }
            return;
        }

        try {
            await axios.delete(`/api/streams/${id}`, {
                data: { confirmation: confirmation }
            });
            setStreams(streams.filter(s => s._id !== id));
        } catch (err) {
            console.error(err);
            alert(err.response?.data?.error || 'Error deleting stream');
        }
    };

    return (
        <div className="min-h-screen p-8 relative">
            {/* Decorative Background */}
            <div className="fixed top-0 left-0 w-full h-[500px] bg-gradient-to-b from-primary/10 to-transparent pointer-events-none" />
            <div className="fixed -top-40 -right-40 w-96 h-96 bg-accent/20 rounded-full blur-[128px] pointer-events-none" />
            <div className="fixed bottom-0 left-20 w-80 h-80 bg-primary/10 rounded-full blur-[100px] pointer-events-none" />

            <header className="relative z-10 flex flex-col md:flex-row justify-between items-center mb-12 gap-6">
                <div className="flex items-center gap-4">
                    <div className="bg-gradient-to-br from-primary to-accent p-3 rounded-2xl shadow-lg shadow-primary/25">
                        <LayoutDashboard className="text-white w-6 h-6" />
                    </div>
                    <div>
                        <h1 className="text-3xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-white via-white to-white/70 tracking-tight">
                            HLS Monitor
                        </h1>
                        <p className="text-secondary text-sm font-medium tracking-wide uppercase mt-1">Real-time Stream Analysis</p>
                    </div>
                </div>

                <div className="flex items-center gap-4 w-full md:w-auto">
                    <button
                        onClick={toggleMute}
                        className={`p-3 rounded-xl transition-colors ${isGlobalMute ? 'bg-rose-500/20 text-rose-400' : 'bg-white/5 text-white/70 hover:text-white'}`}
                        title={isGlobalMute ? "Unmute Alarms" : "Mute Alarms"}
                    >
                        {isGlobalMute ? <VolumeX size={20} /> : <Volume2 size={20} />}
                    </button>

                    <button
                        onClick={() => setIsModalOpen(true)}
                        className="group relative px-6 py-3 bg-primary rounded-xl font-semibold text-white shadow-xl shadow-primary/30 hover:shadow-primary/50 transition-all hover:-translate-y-0.5 active:translate-y-0 overflow-hidden w-full md:w-auto"
                    >
                        <div className="flex items-center justify-center gap-2 relative z-10">
                            <Plus size={18} /> Add Stream
                        </div>
                    </button>
                </div>
            </header>

            {loading ? (
                <div className="flex items-center justify-center h-64">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                </div>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8 relative z-10 pb-20">
                    {streams.map(stream => (
                        <div key={stream._id} onClick={() => handleAcknowledge(stream._id)} className="relative group">
                            {/* Visual Alarm Indicator */}
                            {activeAlarms.has(stream._id) && !isGlobalMute && (
                                <div className="absolute -inset-1 bg-gradient-to-r from-rose-500 via-red-500 to-rose-500 rounded-3xl opacity-75 blur-md animate-pulse pointer-events-none" />
                            )}
                            <StreamCard stream={stream} onDelete={handleDelete} />
                        </div>
                    ))}

                    {streams.length === 0 && (
                        <div className="col-span-full py-32 text-center text-white/30 border-2 border-dashed border-white/5 rounded-3xl bg-surface/20 backdrop-blur-sm">
                            <Activity className="w-16 h-16 mx-auto mb-4 opacity-50" />
                            <h3 className="text-xl font-semibold text-white/70 mb-2">No Active Monitors</h3>
                            <p>Add a new HLS stream to begin monitoring.</p>
                        </div>
                    )}
                </div>
            )}

            {/* Add Stream Modal */}
            {isModalOpen && (
                <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-50 p-4">
                    <div className="glass-panel w-full max-w-lg p-8">
                        <h2 className="text-2xl font-bold mb-6">Add New Monitor</h2>
                        <form onSubmit={handleAddStream} className="space-y-6">
                            <div>
                                <label className="block text-xs font-bold text-secondary uppercase tracking-wider mb-2">Stream Name</label>
                                <input
                                    type="text"
                                    placeholder="e.g. Production Live 01"
                                    required
                                    className="w-full bg-black/20 border border-white/10 rounded-xl p-3 text-white placeholder:text-white/20 focus:border-primary focus:outline-none"
                                    value={newStream.name}
                                    onChange={e => setNewStream({ ...newStream, name: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="block text-xs font-bold text-secondary uppercase tracking-wider mb-2">HLS URL</label>
                                <input
                                    type="url"
                                    placeholder="https://example.com/stream.m3u8"
                                    required
                                    className="w-full bg-black/20 border border-white/10 rounded-xl p-3 text-white placeholder:text-white/20 focus:border-primary focus:outline-none"
                                    value={newStream.url}
                                    onChange={e => setNewStream({ ...newStream, url: e.target.value })}
                                />
                            </div>
                            <div className="flex justify-end gap-3 mt-8">
                                <button type="button" onClick={() => setIsModalOpen(false)} className="px-6 py-2.5 text-white/70 hover:text-white">Cancel</button>
                                <button type="submit" className="bg-primary px-6 py-2.5 rounded-lg font-semibold">Create</button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
}

function App() {
    useEffect(() => {
        const trackVisitor = async () => {
            // Get or create Visitor ID
            let visitorId = localStorage.getItem('hls_visitor_id');
            if (!visitorId) {
                if (typeof crypto !== 'undefined' && crypto.randomUUID) {
                    visitorId = crypto.randomUUID();
                } else {
                    // Fallback for older browsers
                    visitorId = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                        return v.toString(16);
                    });
                }
                localStorage.setItem('hls_visitor_id', visitorId);
            }

            try {
                await axios.post('/api/visitors', {
                    visitorId,
                    screen: {
                        width: window.screen.width,
                        height: window.screen.height,
                        colorDepth: window.screen.colorDepth,
                        pixelRatio: window.devicePixelRatio
                    },
                    metadata: {
                        referrer: document.referrer,
                        language: navigator.language,
                        userAgent: navigator.userAgent
                    }
                });
            } catch (err) {
                console.error('Visitor tracking failed', err);
            }
        };

        trackVisitor();
    }, []);

    return (
        <BrowserRouter>
            <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/stream/:id" element={<StreamDetail />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
