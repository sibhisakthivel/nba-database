"""
Test script for ingestion/potential_ast.py

This script validates the potential assists ingestion functions before
creating the database table. It performs a dry-run test that shows what
data would be inserted without actually inserting into the database.
"""

import sys
from pathlib import Path
import time
import pandas as pd

# Add parent directory to path to ensure imports work
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ingestion.potential_ast import (
    _format_date_for_api,
    _snake_case_columns,
    get_all_game_ids_from_api,
    get_ingested_game_ids_from_db,
    get_game_ids_with_dates,
    get_players_for_game,
    ingest_potential_ast_for_date,
    ingest_potential_ast_for_game_ids,
)
from nba_api.stats.endpoints import LeagueDashPtStats


# Config
SEASON = "2025-26"
SEASON_TYPE = "Regular Season"
SLEEP_SECONDS = 1.5


def test_date_formatting():
    """Test the date formatting function"""
    print("=" * 60)
    print("TEST 1: Date Formatting")
    print("=" * 60)
    
    test_cases = [
        "2025-10-21",
        "2025-01-25",
        "10/21/2025",
        pd.Timestamp("2025-10-21"),
    ]
    
    for test_date in test_cases:
        result = _format_date_for_api(test_date)
        print(f"  Input: {test_date} ({type(test_date).__name__})")
        print(f"  Output: {result}")
        print()
    
    print("✅ Date formatting test complete\n")


def test_get_game_ids():
    """Test getting game IDs from API"""
    print("=" * 60)
    print("TEST 2: Get Game IDs from API")
    print("=" * 60)
    
    try:
        game_ids = get_all_game_ids_from_api(SEASON, SEASON_TYPE)
        print(f"  Found {len(game_ids)} game IDs")
        print(f"  Sample game IDs: {sorted(list(game_ids))[:5]}")
        print("✅ Game ID retrieval test complete\n")
        return game_ids
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return set()


def test_get_game_ids_with_dates():
    """Test getting game IDs with dates"""
    print("=" * 60)
    print("TEST 3: Get Game IDs with Dates")
    print("=" * 60)
    
    try:
        games_df = get_game_ids_with_dates(SEASON, SEASON_TYPE)
        print(f"  Found {len(games_df)} game records")
        print(f"  Columns: {list(games_df.columns)}")
        print(f"\n  Sample data:")
        print(games_df.head().to_string())
        
        # Show date distribution
        if 'GAME_DATE' in games_df.columns:
            unique_dates = games_df['GAME_DATE'].nunique()
            print(f"\n  Unique game dates: {unique_dates}")
            print(f"  Date range: {games_df['GAME_DATE'].min()} to {games_df['GAME_DATE'].max()}")
        
        print("✅ Game IDs with dates test complete\n")
        return games_df
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_get_ingested_game_ids():
    """Test getting ingested game IDs from DB"""
    print("=" * 60)
    print("TEST 4: Get Ingested Game IDs from DB")
    print("=" * 60)
    
    try:
        ingested_ids = get_ingested_game_ids_from_db()
        print(f"  Found {len(ingested_ids)} ingested game IDs")
        if len(ingested_ids) > 0:
            print(f"  Sample: {sorted(list(ingested_ids))[:5]}")
        else:
            print("  (Table may not exist yet - this is expected)")
        print("✅ Ingested game IDs test complete\n")
        return ingested_ids
    except Exception as e:
        print(f"❌ Error: {e}")
        print("  (This is expected if the table doesn't exist yet)\n")
        return set()


def test_get_players_for_game():
    """Test getting players for a specific game"""
    print("=" * 60)
    print("TEST 5: Get Players for a Game")
    print("=" * 60)
    
    # Get a sample game ID
    try:
        game_ids = get_all_game_ids_from_api(SEASON, SEASON_TYPE)
        if not game_ids:
            print("  ⚠️  No game IDs found, skipping test")
            return
        
        sample_game_id = sorted(list(game_ids))[0]
        print(f"  Testing with game_id: {sample_game_id}")
        
        players = get_players_for_game(sample_game_id)
        print(f"  Found {len(players)} players in this game")
        if len(players) > 0:
            print(f"  Sample player IDs: {sorted(list(players))[:10]}")
        print("✅ Get players for game test complete\n")
        return sample_game_id, players
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, set()


def get_test_date_with_box_score_data():
    """Helper function to get a date that has box score data"""
    games_df = get_game_ids_with_dates(SEASON, SEASON_TYPE)
    if games_df is None or len(games_df) == 0:
        return None, None
    
    if 'GAME_DATE' not in games_df.columns:
        return None, None
    
    # Try to find a date with existing box score data
    from db.connection import get_engine
    from sqlalchemy import text
    
    engine = get_engine()
    test_date = None
    try:
        query = text("""
            SELECT DISTINCT b.game_id, l.game_date
            FROM raw.box_score_traditional_v3 b
            JOIN raw.league_game_log l ON b.game_id = l.game_id
            ORDER BY l.game_date DESC
            LIMIT 1
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            if row:
                test_date = row[1]
    except Exception as e:
        pass
    
    if test_date:
        # Convert date object to string for comparison
        if hasattr(test_date, 'strftime'):
            date_str = test_date.strftime('%Y-%m-%d')
        else:
            date_str = str(test_date)
        return date_str, games_df
    else:
        # Fallback: use most recent date
        recent_date = games_df['GAME_DATE'].max()
        return recent_date, games_df


def test_api_call():
    """Test the LeagueDashPtStats API call"""
    print("=" * 60)
    print("TEST 6: LeagueDashPtStats API Call")
    print("=" * 60)
    
    try:
        # Get a date with box score data (same as Test 7 will use)
        recent_date, games_df = get_test_date_with_box_score_data()
        if recent_date is None:
            print("  ⚠️  No games found, skipping test")
            return None
        
        date_formatted = _format_date_for_api(recent_date)
        print(f"  Testing with date: {recent_date} (formatted: {date_formatted})")
        
        print(f"  Making API call...")
        endpoint = LeagueDashPtStats(
            pt_measure_type="Passing",
            season=SEASON,
            season_type_all_star=SEASON_TYPE,
            player_or_team="Player",
            date_from_nullable=date_formatted,
            date_to_nullable=date_formatted
        )
        
        passing_df = endpoint.get_data_frames()[0]
        print(f"  ✅ API call successful!")
        print(f"  Retrieved {len(passing_df)} player records")
        print(f"  Columns: {list(passing_df.columns)}")
        
        # Show sample data
        if len(passing_df) > 0:
            print(f"\n  Sample data (first 3 rows):")
            print(passing_df.head(3).to_string())
            
            # Check for key columns
            key_cols = ['POTENTIAL_AST', 'AST', 'PASSES_MADE', 'PLAYER_ID', 'PERSON_ID']
            found_cols = [col for col in key_cols if col in passing_df.columns]
            print(f"\n  Key columns found: {found_cols}")
        
        print("✅ API call test complete\n")
        return passing_df, recent_date
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_dry_run_ingestion():
    """Test the full ingestion logic in dry-run mode"""
    print("=" * 60)
    print("TEST 7: Dry-Run Ingestion (No DB Insert)")
    print("=" * 60)
    
    try:
        # Get the same date as Test 6 (with box score data)
        recent_date, games_df = get_test_date_with_box_score_data()
        if recent_date is None or games_df is None:
            print("  ⚠️  No games found, skipping test")
            return
        
        if 'GAME_DATE' not in games_df.columns:
            print("  ⚠️  GAME_DATE column not found")
            return
        
        game_ids_for_date = games_df[games_df['GAME_DATE'] == recent_date]['GAME_ID'].tolist()
        print(f"  Using same date as Test 6: {recent_date}")
        print(f"  Found {len(game_ids_for_date)} game(s) for this date")
        print(f"  Testing with date: {recent_date} (type: {type(recent_date).__name__})")
        print(f"  Game IDs for this date: {game_ids_for_date}")
        
        # Debug: Show date types in games_df
        if len(game_ids_for_date) == 0 and len(games_df) > 0:
            print(f"  ⚠️  DEBUG: Checking date format mismatch...")
            sample_dates = games_df['GAME_DATE'].head(5).tolist()
            print(f"    Sample dates from API: {sample_dates} (types: {[type(d).__name__ for d in sample_dates]})")
            print(f"    Looking for date: {recent_date} (type: {type(recent_date).__name__})")
            # Try to find matches with different formats
            if hasattr(recent_date, 'strftime'):
                date_str = recent_date.strftime('%Y-%m-%d')
                matches = games_df[games_df['GAME_DATE'] == date_str]['GAME_ID'].tolist()
                if matches:
                    print(f"    ✅ Found {len(matches)} matches when converted to string!")
                    game_ids_for_date = matches
        
        # Format date for API
        date_formatted = _format_date_for_api(recent_date)
        
        # Make API call
        print(f"  Making API call...")
        endpoint = LeagueDashPtStats(
            pt_measure_type="Passing",
            season=SEASON,
            season_type_all_star=SEASON_TYPE,
            player_or_team="Player",
            date_from_nullable=date_formatted,
            date_to_nullable=date_formatted
        )
        
        passing_df = endpoint.get_data_frames()[0]
        
        if len(passing_df) == 0:
            print("  ⚠️  No data returned from API")
            return
        
        # Normalize columns
        passing_df = _snake_case_columns(passing_df)
        
        # Get player-to-game_id mapping
        player_to_game_ids = {}
        total_players_found = 0
        for game_id in game_ids_for_date:
            players_in_game = get_players_for_game(game_id)
            total_players_found += len(players_in_game)
            if len(players_in_game) > 0:
                print(f"    Game {game_id}: {len(players_in_game)} players found")
            for player_id in players_in_game:
                if player_id not in player_to_game_ids:
                    player_to_game_ids[player_id] = set()
                player_to_game_ids[player_id].add(game_id)
        
        print(f"  Found {len(player_to_game_ids)} unique players across {len(game_ids_for_date)} game(s)")
        print(f"  Total player-game mappings: {total_players_found}")
        
        if len(player_to_game_ids) == 0:
            print(f"  ⚠️  DEBUG: Checking if any of these game_ids exist in box_score_traditional_v3...")
            from db.connection import get_engine
            from sqlalchemy import text
            engine = get_engine()
            try:
                # Check using IN clause for PostgreSQL
                game_ids_str = "', '".join(game_ids_for_date)
                query = text(f"""
                    SELECT game_id, COUNT(*) as player_count
                    FROM raw.box_score_traditional_v3
                    WHERE game_id IN ('{game_ids_str}')
                    GROUP BY game_id
                """)
                with engine.connect() as conn:
                    result = conn.execute(query, {"game_ids": game_ids_for_date})
                    rows = result.fetchall()
                    if rows:
                        print(f"    Found {len(rows)} game(s) in box_score_traditional_v3:")
                        for row in rows:
                            print(f"      {row[0]}: {row[1]} players")
                    else:
                        print(f"    ❌ None of these game_ids exist in box_score_traditional_v3")
                        print(f"    This means box_score_traditional_v3 needs to be updated for these games first")
            except Exception as e:
                print(f"    Error checking: {e}")
        
        # Find player_id column
        player_id_col = None
        for col in ['player_id', 'person_id', 'PLAYER_ID', 'PERSON_ID']:
            if col in passing_df.columns:
                player_id_col = col
                break
        
        if not player_id_col:
            print("  ⚠️  Could not find player_id column")
            return
        
        print(f"  Using player ID column: {player_id_col}")
        
        # Prepare rows (dry-run - don't insert)
        rows_to_insert = []
        
        for _, row in passing_df.iterrows():
            player_id = int(row[player_id_col])
            game_ids_for_player = player_to_game_ids.get(player_id, set())
            
            if not game_ids_for_player:
                continue
            
            for game_id in game_ids_for_player:
                row_data = {
                    'game_id': game_id,
                    'game_date': recent_date,
                    'person_id': player_id,
                }
                
                for col in passing_df.columns:
                    if col not in [player_id_col, 'team_id', 'TEAM_ID', 'team_name', 'TEAM_NAME', 
                                  'team_abbreviation', 'TEAM_ABBREVIATION', 'player_name', 'PLAYER_NAME']:
                        row_data[col] = row[col]
                
                rows_to_insert.append(row_data)
        
        print(f"\n  ✅ Would insert {len(rows_to_insert)} rows")
        
        if len(rows_to_insert) > 0:
            sample_df = pd.DataFrame(rows_to_insert[:5])
            print(f"\n  Sample rows that would be inserted:")
            print(sample_df.to_string())
            
            # Show detailed column information for table design
            print(f"\n  📋 Column Information for Table Design:")
            print(f"  {'=' * 70}")
            
            sample_df = pd.DataFrame(rows_to_insert)
            
            print(f"\n  Column Name                    | Data Type      | Sample Value")
            print(f"  {'-' * 70}")
            
            for col in sample_df.columns:
                dtype = str(sample_df[col].dtype)
                sample_val = sample_df[col].iloc[0] if len(sample_df) > 0 else None
                
                # Format sample value for display
                if pd.isna(sample_val):
                    sample_str = "NULL"
                elif isinstance(sample_val, (int, float)):
                    sample_str = str(sample_val)
                else:
                    sample_str = str(sample_val)[:30]  # Truncate long strings
                
                # Map pandas dtypes to SQL types
                sql_type_map = {
                    'int64': 'INTEGER',
                    'float64': 'DOUBLE PRECISION',
                    'object': 'TEXT',
                    'string': 'TEXT',
                    'bool': 'BOOLEAN',
                }
                sql_type = sql_type_map.get(dtype, 'TEXT')
                
                print(f"  {col:30} | {sql_type:13} | {sample_str}")
            
            print(f"\n  Total columns: {len(sample_df.columns)}")
            print(f"  Total rows (sample): {len(sample_df)}")
            
            # Show all column names in a list format
            print(f"\n  All column names (for COPY):")
            print(f"  {', '.join(sample_df.columns.tolist())}")
            
            # Show primary key suggestion
            print(f"\n  💡 Suggested Primary Key: (game_id, person_id)")
            print(f"  💡 Suggested Indexes: game_date, person_id")
        
        print("✅ Dry-run ingestion test complete\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("TESTING POTENTIAL ASSISTS INGESTION FUNCTIONS")
    print("=" * 60 + "\n")
    
    # Test 1: Date formatting
    test_date_formatting()
    time.sleep(0.5)
    
    # Test 2: Get game IDs
    game_ids = test_get_game_ids()
    time.sleep(SLEEP_SECONDS)
    
    # Test 3: Get game IDs with dates
    games_df = test_get_game_ids_with_dates()
    time.sleep(SLEEP_SECONDS)
    
    # Test 4: Get ingested game IDs (may fail if table doesn't exist)
    ingested_ids = test_get_ingested_game_ids()
    
    # Test 5: Get players for a game
    sample_game_id, players = test_get_players_for_game()
    
    # Test 6: API call
    passing_df, test_date = test_api_call()
    time.sleep(SLEEP_SECONDS)
    
    # Test 7: Dry-run ingestion
    test_dry_run_ingestion()
    
    print("=" * 60)
    print("ALL TESTS COMPLETE")
    print("=" * 60)
    print("\n📋 Summary:")
    print("  - If all tests passed, the functions are ready to use")
    print("  - Create the raw.potential_ast table in your database")
    print("  - Then integrate into update_database.py")
    print()


if __name__ == "__main__":
    main()
