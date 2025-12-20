# HLS Monitor

A real-time HLS stream monitoring solution with live analytics, signal visualization, and health tracking.

**Live Demo:** https://hls-monitor.onrender.com/

## Features

- Real-time HLS stream monitoring with 7-second polling intervals
- Live signal strength visualization with VU meters
- Auto-updating thumbnails from stream frames
- Health scoring system (0-100) based on errors and stability
- Downloadable daily log files with date selection
- Detailed video, audio, and container analysis via FFprobe
- Client-side HLS playback without server load
- Audit logging for all user operations
- Sliding window metrics for accurate health calculation
- Custom dark theme with modern UI

## Tech Stack

- Backend: Node.js, Express, MongoDB, Socket.io
- Frontend: React, Vite, Tailwind CSS, Recharts
- Analysis: FFmpeg/FFprobe

## Screenshots

The dashboard displays all monitored streams with live thumbnails and health scores. Click any stream to view detailed analytics including:

- Live signal strength meters
- Historical bitrate and signal graphs
- Video/Audio codec information
- Error logs with date filtering

## License

MIT
