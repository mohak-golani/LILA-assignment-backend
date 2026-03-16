import pandas as pd
import pyarrow.parquet as pq
import os
import json
from typing import Dict, List, Tuple
from pathlib import Path

# Map configurations for coordinate conversion
MAP_CONFIGS = {
    "AmbroseValley": {"scale": 900, "originX": -370, "originZ": -473},
    "GrandRift": {"scale": 581, "originX": -290, "originZ": -290},
    "Lockdown": {"scale": 1000, "originX": -500, "originZ": -500}
}

def world_to_pixel(x: float, z: float, map_id: str) -> Tuple[int, int]:
    """Convert world coordinates to pixel coordinates"""
    config = MAP_CONFIGS.get(map_id)
    if not config:
        return 0, 0
    
    u = (x - config["originX"]) / config["scale"]
    v = (z - config["originZ"]) / config["scale"]
    
    pixel_x = round(u * 1024)
    pixel_y = round((1 - v) * 1024)  # Y is flipped
    
    return pixel_x, pixel_y

def is_bot(user_id: str) -> bool:
    """Check if user_id is a bot (numeric) or human (UUID)"""
    return str(user_id).strip().isdigit()

def get_date_from_folder(folder: str) -> str:
    """Extract date from folder name"""
    return folder.replace('February_', 'Feb-')

def process_parquet_file(file_path: str, folder: str) -> List[Dict]:
    """Read and process a single parquet file"""
    try:
        # Read parquet file using pyarrow
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        # Decode event bytes to strings
        df['event'] = df['event'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else str(x)
        )
        
        # Add pixel coordinates using only x and z (ignore y which is elevation)
        df[['pixelX', 'pixelY']] = df.apply(
            lambda row: pd.Series(world_to_pixel(row['x'], row['z'], row['map_id'])), 
            axis=1
        )
        
        # Add derived fields
        df['isBot'] = df['user_id'].apply(lambda x: is_bot(str(x)))
        df['date'] = get_date_from_folder(folder)
        
        # Convert to list of dictionaries
        events = []
        for _, row in df.iterrows():
            event = {
                'user_id': str(row['user_id']),
                'match_id': str(row['match_id']),
                'map_id': str(row['map_id']),
                'x': float(row['x']) if pd.notna(row['x']) else 0.0,
                'y': float(row['y']) if pd.notna(row['y']) else 0.0,  # Keep y for reference but don't use in mapping
                'z': float(row['z']) if pd.notna(row['z']) else 0.0,
                'pixelX': int(row['pixelX']),
                'pixelY': int(row['pixelY']),
                'timestamp': str(row['ts']),  # Keep raw timestamp for now
                'event': str(row['event']),
                'isBot': bool(row['isBot']),
                'date': str(row['date'])
            }
            events.append(event)
        
        return events
        
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return []

def generate_heatmap_data(events: List[Dict], map_id: str) -> Dict:
    """Generate heatmap data from events"""
    heatmaps = {
        'kills': [],
        'deaths': [],
        'traffic': []
    }
    
    for event in events:
        if event['map_id'] != map_id:
            continue
            
        point = {
            'pixelX': event['pixelX'],
            'pixelY': event['pixelY'],
            'x': event['x'],
            'z': event['z'],
            'intensity': 1
        }
        
        # Kill heatmap
        if event['event'] in ['Kill', 'BotKilled']:
            heatmaps['kills'].append(point)
        
        # Death heatmap
        if event['event'] in ['Killed', 'BotKill', 'KilledByStorm']:
            heatmaps['deaths'].append(point)
        
        # Traffic heatmap (all position events)
        if event['event'] in ['Position', 'BotPosition']:
            heatmaps['traffic'].append(point)
    
    return heatmaps

def preprocess_all_data() -> Dict:
    """Main preprocessing function"""
    # Assuming the folder structure:
    # backend/
    #   data_service.py
    #   player_data/
    #       February_10/
    #       February_11/
    #       ...
    # Point the data directory at the `player_data` folder next to this file.
    data_dir = Path(__file__).parent / "player_data"
    folders = ['February_10', 'February_11', 'February_12', 'February_13', 'February_14']
    
    print('📁 Processing folders:', folders)
    
    all_events = []
    matches_dict = {}
    events_by_match = {}
    
    # Process each folder
    for folder in folders:
        folder_path = data_dir / folder
        
        if not folder_path.exists():
            print(f'⚠️  Folder {folder} not found, skipping...')
            continue
        
        print(f'📂 Processing {folder}...')
        # Read all files in the folder (including .nakama-0 files without extension filtering)
        files = [f for f in folder_path.iterdir() if f.is_file() and not f.name.startswith('.')]
        
        processed_files = 0
        for file_path in files:
            file_events = process_parquet_file(str(file_path), folder)
            
            if file_events:
                all_events.extend(file_events)
                
                # Group events by match_id
                for event in file_events:
                    match_id = event['match_id']
                    if match_id not in events_by_match:
                        events_by_match[match_id] = []
                    events_by_match[match_id].append(event)
                
                # Create match metadata
                first_event = file_events[0]
                match_id = first_event['match_id']
                
                if match_id not in matches_dict:
                    matches_dict[match_id] = {
                        'match_id': match_id,
                        'map_id': first_event['map_id'],
                        'date': first_event['date'],
                        'players': set(),
                        'bots': set(),
                        'totalEvents': 0,
                        'duration': 0
                    }
                
                match_data = matches_dict[match_id]
                for event in file_events:
                    if event['isBot']:
                        match_data['bots'].add(event['user_id'])
                    else:
                        match_data['players'].add(event['user_id'])
                    match_data['totalEvents'] += 1
                
                processed_files += 1
        
        print(f'✅ Processed {processed_files} files from {folder}')
    
    # Sort events by timestamp within each match
    for match_id, events in events_by_match.items():
        # Sort by raw timestamp first
        events.sort(key=lambda x: pd.to_datetime(x['timestamp']))
        
        # Get the first timestamp as match start
        if events:
            first_timestamp = pd.to_datetime(events[0]['timestamp'])
            match_data = matches_dict[match_id]
            
            # Convert all timestamps to match-relative milliseconds
            for event in events:
                event_timestamp = pd.to_datetime(event['timestamp'])
                relative_ms = int((event_timestamp - first_timestamp).total_seconds() * 1000)
                event['timestamp'] = relative_ms  # Overwrite with match-relative time
            
            # Re-sort by match-relative timestamps
            events.sort(key=lambda x: x['timestamp'])
            
            # Update match duration using the converted timestamps
            match_data['duration'] = events[-1]['timestamp']  # Last event's relative time
            match_data['players'] = len(match_data['players'])
            match_data['bots'] = len(match_data['bots'])
    
    # Generate heatmaps for each map
    heatmaps = {}
    maps = ['AmbroseValley', 'GrandRift', 'Lockdown']
    
    print('🔥 Generating heatmaps...')
    for map_id in maps:
        map_events = [event for event in all_events if event['map_id'] == map_id]
        if map_events:
            heatmaps[map_id] = generate_heatmap_data(map_events, map_id)
            print(f'📊 Generated heatmap for {map_id}: {len(map_events)} events')
    
    return {
        'matches': list(matches_dict.values()),
        'events': events_by_match,
        'heatmaps': heatmaps
    }