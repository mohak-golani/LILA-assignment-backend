# LILA BLACK Backend (Python FastAPI)

A fast, modern API backend for processing and serving LILA BLACK player behavior data.

## Features

- ✅ **Native Parquet Support**: Uses pandas + pyarrow for reliable parquet reading
- ✅ **FastAPI**: Modern, fast API framework with automatic documentation
- ✅ **Data Processing**: Coordinate transformation, event decoding, heatmap generation
- ✅ **RESTful Endpoints**: Clean API design with pagination support
- ✅ **CORS Enabled**: Ready for frontend integration
- ✅ **Automatic Docs**: Interactive API documentation at `/docs`

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Test parquet reading (optional)
python test_parquet.py
```

## Usage

```bash
# Start the server
python main.py

# Or with uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 3001 --reload
```

Server will be available at: http://localhost:3001

## API Endpoints

### Matches
- `GET /api/matches` - List all matches
- `GET /api/matches?map_id=AmbroseValley` - Filter by map
- `GET /api/matches?date=Feb-10` - Filter by date

### Events  
- `GET /api/events/{match_id}` - Get events for a match
- `GET /api/events/{match_id}?page=1&limit=5000` - Paginated events

### Heatmaps
- `GET /api/heatmap/{map_id}` - Get heatmap data
- `GET /api/heatmap/{map_id}?type=kills` - Specific heatmap (kills, deaths, traffic)

### Configuration
- `GET /api/maps` - Get map configurations
- `GET /health` - Health check
- `GET /docs` - Interactive API documentation

## Data Processing

On startup, the server:

1. **Reads** all parquet files from February_10-14 folders
2. **Decodes** event bytes to readable strings
3. **Transforms** world coordinates to pixel coordinates
4. **Groups** events by match_id with timestamps
5. **Generates** heatmap data for kills, deaths, and traffic
6. **Caches** everything in memory for fast API responses

## Architecture

```
backend/
├── main.py              # FastAPI app and routes
├── data_service.py      # Data processing logic
├── test_parquet.py      # Test script
├── requirements.txt     # Dependencies
└── README.md           # This file
```

## Performance

- **Startup Time**: ~10-30 seconds (processes all parquet files)
- **API Response**: <100ms (data cached in memory)
- **Memory Usage**: ~500MB-1GB (depends on data size)
- **Concurrent Users**: Supports multiple simultaneous requests

## Development

```bash
# Run in development mode with auto-reload
uvicorn main:app --reload --port 3001

# View API docs
open http://localhost:3001/docs

# Test endpoints
curl http://localhost:3001/api/matches
curl http://localhost:3001/health
```