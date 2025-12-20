import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { Trash2, Signal, AlertTriangle, ExternalLink } from 'lucide-react';

const StreamCard = ({ stream, onDelete }) => {
    const [imageKey, setImageKey] = useState(Date.now());

    useEffect(() => {
        if (stream.thumbnail) {
            setImageKey(Date.now());
        }
    }, [stream.thumbnail]);

    const statusConfig = {
        online: { color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', glow: 'shadow-emerald-500/20', label: 'LIVE' },
        offline: { color: 'text-slate-400', bg: 'bg-slate-500/10', border: 'border-slate-500/20', glow: '', label: 'OFFLINE' },
        error: { color: 'text-rose-400', bg: 'bg-rose-500/10', border: 'border-rose-500/30', glow: 'shadow-rose-500/20', label: 'ERROR' },
        stale: { color: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/30', glow: 'shadow-amber-500/20', label: 'STALE' }
    };

    const current = statusConfig[stream.status] || statusConfig.offline;
    const health = stream.health || {};

    // Calculate health score - Based on status and recent SIGNIFICANT errors
    const calculateHealthScore = () => {
        let score = 100;

        // Status penalties (hard caps)
        if (stream.status === 'error') return 60;
        if (stream.status === 'offline') return 50;
        if (health.isStale) score -= 15;

        // Recent errors penalty - now only counts significant gaps (3+)
        // 0 errors = 100, 1-2 = 95-100, 3-5 = 85-95, 6+ = 70-85
        const errors = health.recentErrors || 0;
        if (errors > 0) {
            // Each error costs 3 points, max penalty 30
            score -= Math.min(errors * 3, 30);
        }

        return Math.max(0, Math.min(100, score));
    };

    const healthScore = calculateHealthScore();
    const healthColor = healthScore >= 80 ? 'bg-emerald-500' : healthScore >= 50 ? 'bg-amber-500' : 'bg-rose-500';

    return (
        <div className={`glass-panel group relative overflow-hidden hover:border-primary/40 flex flex-col h-full shadow-lg transition-all hover:scale-[1.02] ${current.glow ? `shadow-lg ${current.glow}` : ''}`}>

            {/* Clickable Thumbnail */}
            <Link to={`/stream/${stream._id}`} className="relative aspect-video bg-black/80 border-b border-white/5 cursor-pointer">
                {stream.thumbnail ? (
                    <img key={imageKey} src={stream.thumbnail} alt="Preview" className="w-full h-full object-cover" />
                ) : (
                    <div className="flex flex-col items-center justify-center h-full text-white/10 gap-2">
                        <Signal className="w-12 h-12" />
                        <span className="text-xs font-mono uppercase tracking-widest">No Signal</span>
                    </div>
                )}

                {/* Health Score Badge */}
                <div className={`absolute top-2 left-2 w-8 h-8 ${healthColor} rounded-full flex items-center justify-center text-white font-bold text-xs shadow-lg`}>
                    {healthScore}
                </div>

                {/* Status Badge */}
                <div className={`absolute top-2 right-2 px-2 py-1 rounded text-[10px] font-bold tracking-wider border backdrop-blur-sm flex items-center gap-1 ${current.bg} ${current.color} ${current.border}`}>
                    <span className={`w-1.5 h-1.5 rounded-full bg-current ${stream.status === 'online' ? 'animate-pulse' : ''}`} />
                    {current.label}
                </div>

                {/* Stale Warning */}
                {health.isStale && (
                    <div className="absolute bottom-2 left-2 px-2 py-1 bg-amber-500/90 text-black rounded text-[10px] font-bold flex items-center gap-1">
                        <AlertTriangle size={10} /> STALE
                    </div>
                )}

                {/* View Details Overlay */}
                <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
                    <span className="bg-primary px-4 py-2 rounded-lg text-white font-bold text-sm flex items-center gap-2">
                        <ExternalLink size={14} /> View Details
                    </span>
                </div>
            </Link>

            {/* Info Area */}
            <div className="p-4 flex-1 flex flex-col">
                <div className="flex justify-between items-start mb-3">
                    <Link to={`/stream/${stream._id}`} className="overflow-hidden flex-1 mr-2 hover:opacity-80">
                        <h3 className="font-bold text-base text-white/90 truncate">{stream.name}</h3>
                        <p className="text-[10px] font-mono text-white/30 truncate">{stream.url}</p>
                    </Link>
                    <button
                        onClick={(e) => { e.preventDefault(); onDelete(stream._id); }}
                        className="p-1.5 opacity-0 group-hover:opacity-60 hover:!opacity-100 text-rose-400 hover:bg-rose-500/10 rounded transition-all"
                    >
                        <Trash2 size={14} />
                    </button>
                </div>

                {/* Quick Stats */}
                <div className="grid grid-cols-3 gap-2 text-center">
                    <div className="bg-black/30 rounded p-2 border border-white/5">
                        <div className="text-[9px] uppercase text-white/40 mb-0.5">Sequence</div>
                        <div className="text-sm font-mono text-primary font-bold">{health.mediaSequence ?? '-'}</div>
                    </div>
                    <div className="bg-black/30 rounded p-2 border border-white/5">
                        <div className="text-[9px] uppercase text-white/40 mb-0.5">Segments</div>
                        <div className="text-sm font-mono text-white/80 font-bold">{health.segmentCount ?? '-'}</div>
                    </div>
                    <div className="bg-black/30 rounded p-2 border border-white/5">
                        <div className="text-[9px] uppercase text-white/40 mb-0.5">Errors</div>
                        <div className={`text-sm font-mono font-bold ${health.totalErrors > 0 ? 'text-rose-400' : 'text-emerald-400'}`}>{health.totalErrors ?? 0}</div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default StreamCard;
