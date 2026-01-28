from pathlib import Path
from typing import Set, Dict, List
import sys
import os
import time

# Handle import conflict with nba_api.py in parent directory
# Temporarily remove parent directory from sys.path to import nba_api package
parent_dir = Path(__file__).parent.parent.absolute()
nba_api_file = parent_dir / "nba_api.py"

if nba_api_file.exists():
    parent_dir_str = str(parent_dir)
    was_in_path = parent_dir_str in sys.path
    if was_in_path:
        sys.path.remove(parent_dir_str)
    
    try:
        import pandas as pd
        from nba_api.stats.endpoints import LeagueDashPtStats, LeagueGameLog
    finally:
        # Restore parent_dir for db.connection import
        if was_in_path and parent_dir_str not in sys.path:
            sys.path.insert(0, parent_dir_str)
else:
    import pandas as pd
    from nba_api.stats.endpoints import LeagueDashPtStats, LeagueGameLog

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

# Ensure parent directory is in path for db.connection
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db.connection import get_engine


RAW_CSV_DIR = Path("data/raw/potential_ast")
RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)

# Rate limiting: sleep between API calls (seconds)
SLEEP_BETWEEN_DATES = 1.5


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to snake_case"""
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.lower()
    )
    return df


def _clean_nan_values(value):
    """Convert NaN/None values to None (NULL for database)"""
    import math
    import numpy as np
    
    # Handle pandas NaN, numpy NaN, Python float NaN, and None
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (np.floating, np.integer)) and (np.isnan(value) or np.isinf(value)):
        return None
    return value


def _format_date_for_api(game_date) -> str:
    """
    Convert date from YYYY-MM-DD (or date object) to MM/DD/YYYY format for NBA API.
    Handles string dates, date objects, and datetime objects.
    """
    try:
        # Handle pandas Timestamp or datetime objects
        if hasattr(game_date, 'strftime'):
            date_str = game_date.strftime('%Y-%m-%d')
        else:
            date_str = str(game_date)
        
        # Parse YYYY-MM-DD format
        if '-' in date_str:
            date_parts = date_str.split('-')
            if len(date_parts) == 3:
                return f"{date_parts[1]}/{date_parts[2]}/{date_parts[0]}"
        
        # If already in MM/DD/YYYY format, return as-is
        if '/' in date_str and len(date_str.split('/')) == 3:
            return date_str
        
        return date_str
    except Exception as e:
        print(f"⚠️  Warning: Could not format date {game_date}: {e}")
        return str(game_date)


def get_game_ids_with_dates(season: str, season_type: str) -> pd.DataFrame:
    """
    Get all game_ids with their corresponding game_dates from LeagueGameLog.
    Returns DataFrame with columns: game_id, game_date
    """
    lg = LeagueGameLog(
        season=season,
        season_type_all_star=season_type
    )
    
    games_df = lg.get_data_frames()[0]
    
    # Return unique game_id and game_date pairs
    return games_df[['GAME_ID', 'GAME_DATE']].drop_duplicates()


def get_all_game_dates_from_api(season: str, season_type: str) -> Set[str]:
    """
    Fetch all game_dates available from the NBA API for a given season.
    Returns set of date strings in YYYY-MM-DD format.
    """
    games_df = get_game_ids_with_dates(season, season_type)
    
    # Convert dates to YYYY-MM-DD format
    dates = set()
    for date_val in games_df['GAME_DATE'].unique():
        if pd.isna(date_val):
            continue
        if hasattr(date_val, 'strftime'):
            dates.add(date_val.strftime('%Y-%m-%d'))
        else:
            # Try to parse if it's a string
            date_str = str(date_val)
            dates.add(date_str)
    
    return dates


def get_all_game_ids_from_api(season: str, season_type: str) -> Set[str]:
    """
    Fetch all game_ids available from the NBA API for a given season.
    Uses LeagueGameLog to get all games.
    """
    lg = LeagueGameLog(
        season=season,
        season_type_all_star=season_type
    )
    
    games_df = lg.get_data_frames()[0]
    return set(games_df["GAME_ID"].unique())


def get_ingested_game_ids_from_db() -> Set[str]:
    """
    Fetch distinct game_ids already ingested into raw.potential_ast.
    Assumes table exists with game_id column.
    """
    engine = get_engine()
    
    query = """
        SELECT DISTINCT game_id
        FROM raw.potential_ast
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return {row[0] for row in result}
    except Exception as e:
        # Table might not exist yet, return empty set
        print(f"⚠️  Note: raw.potential_ast table may not exist yet: {e}")
        return set()


def get_ingested_game_dates_from_db() -> Set[str]:
    """
    Fetch distinct game_dates already ingested into raw.potential_ast.
    Returns set of date strings in YYYY-MM-DD format.
    """
    engine = get_engine()
    
    query = """
        SELECT DISTINCT game_date
        FROM raw.potential_ast
        WHERE game_date IS NOT NULL
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Convert date objects to strings in YYYY-MM-DD format
            dates = set()
            for row in result:
                date_val = row[0]
                if hasattr(date_val, 'strftime'):
                    dates.add(date_val.strftime('%Y-%m-%d'))
                else:
                    dates.add(str(date_val))
            return dates
    except Exception as e:
        # Table might not exist yet, return empty set
        print(f"⚠️  Note: raw.potential_ast table may not exist yet: {e}")
        return set()


def get_incomplete_game_ids(season: str, season_type: str) -> Set[str]:
    """
    Find game_ids that are missing potential_ast records for players who should have them.
    Only flags games where players with minutes > 0 are missing from potential_ast.
    This is more accurate than comparing total counts since not all players have passing stats.
    
    Returns set of game_ids that have missing player records.
    """
    engine = get_engine()
    
    query = """
        SELECT DISTINCT bst.game_id
        FROM raw.box_score_traditional_v3 bst
        LEFT JOIN raw.potential_ast pa 
            ON bst.game_id = pa.game_id 
            AND bst.person_id = pa.person_id
        WHERE bst.minutes > 0  -- Only players who actually played
          AND pa.person_id IS NULL  -- Missing from potential_ast
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return {row[0] for row in result}
    except Exception as e:
        print(f"⚠️  Error checking incomplete games: {e}")
        return set()


def get_players_for_game(game_id: str) -> Set[int]:
    """
    Get set of player IDs (person_id) who played in a specific game.
    Uses box_score_traditional_v3 table to determine which players played.
    """
    engine = get_engine()
    
    query = """
        SELECT DISTINCT person_id
        FROM raw.box_score_traditional_v3
        WHERE game_id = :game_id
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"game_id": game_id})
            return {row[0] for row in result}
    except Exception as e:
        print(f"⚠️  Could not get players for game {game_id}: {e}")
        return set()


def ingest_potential_ast_for_date(
    game_date: str,
    game_ids_for_date: List[str],
    season: str,
    season_type: str = "Regular Season"
) -> Dict[str, int]:
    """
    Ingest potential assists data for a specific game date.
    
    Since LeagueDashPtStats returns data by date (not game_id), we:
    1. Query the API for that date
    2. Cross-reference with box_score_traditional_v3 to match players to game_ids
    3. Insert rows with game_id + player_id
    
    Returns dict with counts: {'inserted': X, 'skipped': Y}
    """
    engine = get_engine()
    
    # Format date for API (MM/DD/YYYY)
    date_formatted = _format_date_for_api(game_date)
    
    # 1. Pull data from NBA API for this date
    endpoint = LeagueDashPtStats(
        pt_measure_type="Passing",
        season=season,
        season_type_all_star=season_type,
        player_or_team="Player",
        date_from_nullable=date_formatted,
        date_to_nullable=date_formatted
    )
    
    passing_df = endpoint.get_data_frames()[0]
    
    if len(passing_df) == 0:
        return {'inserted': 0, 'skipped': 0}
    
    # 2. Normalize column names
    passing_df = _snake_case_columns(passing_df)
    
    # 3. Get player-to-game_id mapping for all games on this date
    # We need to know which players played in which game_id
    player_to_game_ids: Dict[int, Set[str]] = {}
    for game_id in game_ids_for_date:
        players_in_game = get_players_for_game(game_id)
        for player_id in players_in_game:
            if player_id not in player_to_game_ids:
                player_to_game_ids[player_id] = set()
            player_to_game_ids[player_id].add(game_id)
    
    # 4. Match API data to game_ids
    # Find player_id column name
    player_id_col = None
    for col in ['player_id', 'person_id', 'PLAYER_ID', 'PERSON_ID']:
        if col in passing_df.columns:
            player_id_col = col
            break
    
    if not player_id_col:
        print(f"⚠️  Could not find player_id column in passing data")
        return {'inserted': 0, 'skipped': 0}
    
    # 5. Prepare data for insertion
    rows_to_insert = []
    
    for _, row in passing_df.iterrows():
        player_id = int(row[player_id_col])
        
        # Get all game_ids this player played in on this date
        game_ids_for_player = player_to_game_ids.get(player_id, set())
        
        if not game_ids_for_player:
            # Player not found in box_score data - skip
            continue
        
        # Create one row per game_id for this player
        for game_id in game_ids_for_player:
            row_data = {
                'game_id': game_id,
                'game_date': game_date,
                'person_id': player_id,
            }
            
            # Add all passing stats columns
            for col in passing_df.columns:
                if col not in [player_id_col, 'team_id', 'TEAM_ID', 'team_name', 'TEAM_NAME', 
                              'team_abbreviation', 'TEAM_ABBREVIATION', 'player_name', 'PLAYER_NAME']:
                    # Use column name as-is (already snake_cased)
                    # Clean NaN values to None (NULL in database)
                    row_data[col] = _clean_nan_values(row[col])
            
            rows_to_insert.append(row_data)
    
    if not rows_to_insert:
        return {'inserted': 0, 'skipped': 0}
    
    # 6. Save raw CSV snapshot (one file per game)
    # Group rows by game_id
    rows_by_game: Dict[str, List[Dict]] = {}
    for row_data in rows_to_insert:
        game_id = row_data['game_id']
        if game_id not in rows_by_game:
            rows_by_game[game_id] = []
        rows_by_game[game_id].append(row_data)
    
    # Save one CSV file per game
    for game_id, game_rows in rows_by_game.items():
        game_df = pd.DataFrame(game_rows)
        csv_path = RAW_CSV_DIR / f"potential_ast_{game_id}.csv"
        game_df.to_csv(csv_path, index=False)
    
    # 7. Insert into database
    # Note: This assumes a table structure like:
    # CREATE TABLE raw.potential_ast (
    #     game_id TEXT NOT NULL,
    #     person_id INTEGER NOT NULL,
    #     game_date DATE,
    #     potential_ast INTEGER,
    #     ast INTEGER,
    #     passes_made INTEGER,
    #     passes_received INTEGER,
    #     ... other columns ...
    #     PRIMARY KEY (game_id, person_id)
    # )
    
    # Build dynamic INSERT statement based on available columns
    if not rows_to_insert:
        return {'inserted': 0, 'skipped': 0}
    
    sample_row = rows_to_insert[0]
    columns = list(sample_row.keys())
    
    # Create INSERT SQL dynamically
    columns_str = ', '.join(columns)
    values_str = ', '.join([f':{col}' for col in columns])
    
    insert_sql = text(f"""
        INSERT INTO raw.potential_ast ({columns_str})
        VALUES ({values_str})
        ON CONFLICT (game_id, person_id) DO NOTHING;
    """)
    
    inserted = 0
    skipped = 0
    
    with engine.begin() as conn:
        for row_data in rows_to_insert:
            # Use savepoint for each row so one failure doesn't abort the whole transaction
            savepoint = conn.begin_nested()
            try:
                result = conn.execute(insert_sql, row_data)
                savepoint.commit()
                # Check if row was actually inserted (not skipped due to conflict)
                # With ON CONFLICT DO NOTHING, result.rowcount will be 0 if conflict occurred
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1  # Conflict - row already exists
            except IntegrityError:
                savepoint.rollback()
                skipped += 1
            except Exception as e:
                # Log error but continue
                savepoint.rollback()
                print(f"⚠️  Error inserting row: {e}")
                skipped += 1
    
    return {'inserted': inserted, 'skipped': skipped}


def ingest_potential_ast_for_game_ids(
    game_ids: List[str],
    season: str,
    season_type: str = "Regular Season"
) -> None:
    """
    Ingest potential assists for a list of game_ids.
    Groups by game_date and processes each date.
    
    This is the main function to call for ingesting potential assists data.
    """
    # Get game dates for these game_ids
    games_df = get_game_ids_with_dates(season, season_type)
    
    # Filter to requested game_ids
    games_df = games_df[games_df['GAME_ID'].isin(game_ids)]
    
    if len(games_df) == 0:
        print("⚠️  No games found for provided game_ids")
        return
    
    # Group by game_date
    games_by_date = games_df.groupby('GAME_DATE')['GAME_ID'].apply(list).to_dict()
    
    print(f"📊 Processing {len(games_by_date)} unique game dates")
    
    total_inserted = 0
    total_skipped = 0
    
    for idx, (game_date, game_ids_for_date) in enumerate(games_by_date.items(), 1):
        print(f"\n[{idx}/{len(games_by_date)}] Processing date {game_date} ({len(game_ids_for_date)} game(s))")
        
        try:
            result = ingest_potential_ast_for_date(
                game_date=game_date,
                game_ids_for_date=game_ids_for_date,
                season=season,
                season_type=season_type
            )
            
            total_inserted += result['inserted']
            total_skipped += result['skipped']
            
            print(f"   ✅ Inserted: {result['inserted']}, Skipped: {result['skipped']}")
            
            # Rate limiting: sleep between dates (one API call per date)
            if idx < len(games_by_date):
                time.sleep(SLEEP_BETWEEN_DATES)
            
        except Exception as e:
            print(f"   ❌ Error processing date {game_date}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n📈 Total: Inserted={total_inserted}, Skipped={total_skipped}")
