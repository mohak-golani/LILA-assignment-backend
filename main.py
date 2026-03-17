from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, List, Any
import uvicorn
import pandas as pd
from datetime import datetime
from data_service import preprocess_all_data, MAP_CONFIGS
import os
from dotenv import load_dotenv
import threading 

load_dotenv()

app = FastAPI(
    title="LILA BLACK Player Data API",
    description="Backend API for player behavior visualization",
    version="1.0.0"
)

# Enable CORS for frontend
FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://localhost:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_BASE_URL],
    allow_credentials=False, #hotfix
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global data storage
processed_data = {
    "matches": [],
    "events": {},
    "heatmaps": {}
}

import threading

@app.on_event("startup")
async def startup_event():
    def load_data():
        global processed_data
        print("🚀 Starting LILA BLACK Data Server...")
        print("📊 Preprocessing parquet data...")

        try:
            processed_data = preprocess_all_data()
            print(f"✅ Loaded {len(processed_data['matches'])} matches")
        except Exception as error:
            print(f"❌ Error preprocessing data: {error}")

    threading.Thread(target=load_data).start()

@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "message": "LILA BLACK Player Data API",
        "version": "1.0.0",
        "matches": len(processed_data["matches"]),
        "status": "healthy"
    }

@app.get("/api/matches")
async def get_matches(
    map_id: Optional[str] = Query(None, description="Filter by map ID"),
    date: Optional[str] = Query(None, description="Filter by date")
):
    """Get all matches with optional filtering"""
    try:
        matches = processed_data["matches"]
        
        if map_id:
            matches = [match for match in matches if match["map_id"] == map_id]
        
        if date:
            matches = [match for match in matches if match["date"] == date]
        
        return {
            "matches": matches,
            "total": len(matches)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/events/{match_id}")
async def get_match_events(
    match_id: str,
    page: int = Query(1, description="Page number"),
    limit: int = Query(5000, description="Events per page")
):
    """Get events for a specific match with pagination"""
    try:
        if match_id not in processed_data["events"]:
            raise HTTPException(status_code=404, detail="Match not found")
        
        events = processed_data["events"][match_id]
        
        # Pagination
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_events = events[start_index:end_index]
        
        return {
            "events": paginated_events,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": len(events),
                "totalPages": (len(events) + limit - 1) // limit,
                "hasNext": end_index < len(events)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/heatmap/{map_id}")
async def get_heatmap_data(
    map_id: str,
    type: str = Query("kills", description="Heatmap type: kills, deaths, traffic")
):
    """Get heatmap data for a specific map"""
    try:
        if map_id not in processed_data["heatmaps"]:
            raise HTTPException(status_code=404, detail="Map not found")
        
        heatmap_data = processed_data["heatmaps"][map_id]
        
        if type not in heatmap_data:
            raise HTTPException(status_code=404, detail="Heatmap type not found")
        
        return {
            "mapId": map_id,
            "type": type,
            "data": heatmap_data[type]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/players/{match_id}")
async def get_match_players(match_id: str):
    """Get list of all players in a match"""
    try:
        if match_id not in processed_data["events"]:
            raise HTTPException(status_code=404, detail="Match not found")
        
        events = processed_data["events"][match_id]
        players = {}
        
        for event in events:
            user_id = event["user_id"]
            if user_id not in players:
                players[user_id] = {
                    "user_id": user_id,
                    "isBot": event["isBot"],
                    "eventCount": 0,
                    "firstSeen": event["timestamp"],
                    "lastSeen": event["timestamp"]
                }
            
            players[user_id]["eventCount"] += 1
            if event["timestamp"] > players[user_id]["lastSeen"]:
                players[user_id]["lastSeen"] = event["timestamp"]
        
        return {
            "match_id": match_id,
            "players": list(players.values()),
            "total_players": len(players)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/journey/{match_id}/{user_id}")
async def get_player_journey(match_id: str, user_id: str):
    """Get a specific player's journey through a match"""
    try:
        if match_id not in processed_data["events"]:
            raise HTTPException(status_code=404, detail="Match not found")
        
        events = processed_data["events"][match_id]
        player_events = [event for event in events if event["user_id"] == user_id]
        
        if not player_events:
            raise HTTPException(status_code=404, detail="Player not found in match")
        
        # Separate position events for path and other events for markers
        position_events = []
        action_events = []
        
        for event in player_events:
            if event["event"] in ["Position", "BotPosition"]:
                position_events.append(event)
            else:
                action_events.append(event)
        
        return {
            "match_id": match_id,
            "user_id": user_id,
            "isBot": player_events[0]["isBot"],
            "path": position_events,
            "actions": action_events,
            "total_events": len(player_events)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/journeys/{match_id}")
async def get_all_player_journeys(
    match_id: str,
    include_bots: bool = Query(True, description="Include bot journeys"),
    include_players: bool = Query(True, description="Include human player journeys")
):
    """Get all player journeys for a match"""
    try:
        if match_id not in processed_data["events"]:
            raise HTTPException(status_code=404, detail="Match not found")
        
        events = processed_data["events"][match_id]
        players_data = {}
        
        # Group events by player
        for event in events:
            user_id = event["user_id"]
            is_bot = event["isBot"]
            
            # Filter by bot/player preference
            if is_bot and not include_bots:
                continue
            if not is_bot and not include_players:
                continue
            
            if user_id not in players_data:
                players_data[user_id] = {
                    "user_id": user_id,
                    "isBot": is_bot,
                    "path": [],
                    "actions": []
                }
            
            if event["event"] in ["Position", "BotPosition"]:
                players_data[user_id]["path"].append(event)
            else:
                players_data[user_id]["actions"].append(event)
        
        return {
            "match_id": match_id,
            "journeys": list(players_data.values()),
            "total_players": len(players_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/maps")
async def get_map_configs():
    """Get map configurations for coordinate conversion"""
    return MAP_CONFIGS

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "matches": len(processed_data["matches"]),
        "timestamp": datetime.now().isoformat()
    }
