"""
Discover and print all NBA API endpoint names.

Note: This file is named nba_api.py which can conflict with the nba_api package.
We remove the current directory from sys.path before importing to avoid conflicts.
"""

import inspect
import sys
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Optional, Dict, Any, List

# Remove current directory from sys.path to avoid import conflict
# (since this file is named nba_api.py, it would conflict with the nba_api package)
current_dir = str(Path(__file__).parent.absolute())
if current_dir in sys.path:
    sys.path.remove(current_dir)

try:
    import nba_api.stats.endpoints as endpoints_module
    from nba_api.stats.endpoints import LeagueGameLog
except ImportError as e:
    print(f"Error importing nba_api: {e}")
    print("Make sure nba-api is installed: pip install nba-api")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error importing nba_api: {e}")
    print(f"Error type: {type(e).__name__}")
    sys.exit(1)


def discover_all_endpoints():
    """Discover all endpoint classes from nba_api.stats.endpoints"""
    endpoint_classes = {}
    
    for name, obj in inspect.getmembers(endpoints_module):
        if inspect.isclass(obj) and name[0].isupper():
            endpoint_classes[name] = obj
    
    return endpoint_classes


def get_sample_game_ids(count: int = 3):
    """Get sample game_ids from LeagueGameLog for testing endpoints"""
    try:
        lg = LeagueGameLog(
            season="2024-25",
            season_type_all_star="Regular Season"
        )
        df = lg.get_data_frames()[0]
        if len(df) > 0:
            game_ids = [str(gid) for gid in df["GAME_ID"].unique()[:count]]
            return game_ids
    except Exception:
        pass
    return []


def get_endpoint_columns(endpoint_class, endpoint_name: str, game_id: str, season: str = "2024-25", timeout: int = 10) -> tuple[Optional[List[str]], Optional[str]]:
    """
    Extract column names from an endpoint.
    Returns (columns_list, error_message)
    """
    # Get constructor signature to understand required parameters
    try:
        sig = inspect.signature(endpoint_class.__init__)
        params = list(sig.parameters.keys())
        params = [p for p in params if p != "self"]
    except Exception as e:
        return None, f"Could not inspect constructor: {e}"
    
    # Try different parameter combinations
    attempts = [
        {"game_id": game_id},
        {"game_id": game_id, "season": season},
    ]
    
    def try_endpoint(kwargs, attempt_num):
        """Try to get data from endpoint"""
        try:
            # Filter kwargs to only include parameters that exist in the signature
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in params}
            
            endpoint_instance = endpoint_class(**filtered_kwargs)
            dataframes = endpoint_instance.get_data_frames()
            
            all_columns = []
            for idx, df in enumerate(dataframes):
                columns = list(df.columns)
                all_columns.extend(columns)
            
            return sorted(set(all_columns)), None  # Success
            
        except TypeError as e:
            # Wrong parameters
            if attempt_num < len(attempts):
                return None, None  # Try next combination
            return None, f"Wrong parameters: {str(e)[:150]}"
        except KeyError as e:
            # API response structure issue (e.g., missing 'resultSet')
            return None, f"API response error: {str(e)[:150]}"
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)[:150]}"
            return None, error_msg
    
    # Try each parameter combination
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        for attempt_num, kwargs in enumerate(attempts, 1):
            try:
                future = executor.submit(try_endpoint, kwargs, attempt_num)
                columns, error_msg = future.result(timeout=timeout)
                
                if columns:
                    return columns, None  # Success
                elif error_msg and attempt_num >= len(attempts):
                    return None, error_msg  # Final attempt failed
                # Otherwise try next combination
                continue
                
            except FuturesTimeoutError:
                # Cancel the future and shutdown executor without waiting
                future.cancel()
                executor.shutdown(wait=False)
                return None, f"Request timed out after {timeout} seconds"
            except Exception as e:
                if attempt_num >= len(attempts):
                    executor.shutdown(wait=False)
                    return None, f"Unexpected error: {type(e).__name__}: {str(e)[:150]}"
                continue
    finally:
        # Always shutdown, but don't wait for threads to finish
        executor.shutdown(wait=False)
    
    return None, "All parameter combinations failed"


# def main():
#     """Main execution"""
#     print("🏀 NBA API BoxScore Endpoints - Column Names")
#     print("=" * 60)
#     
#     # Get sample game_ids for testing
#     print("\n📋 Getting sample game_ids...")
#     game_ids = get_sample_game_ids(count=3)
#     if not game_ids:
#         print("❌ Could not get sample game_ids. Exiting.")
#         sys.exit(1)
#     print(f"✅ Using game_ids: {', '.join(game_ids)}\n")
#     
#     # Discover all endpoints
#     all_endpoints = discover_all_endpoints()
#     
#     # Filter to BoxScore endpoints
#     boxscore_endpoints = {
#         name: endpoint_class 
#         for name, endpoint_class in all_endpoints.items() 
#         if name.startswith("BoxScore")
#     }
#     
#     print(f"Found {len(boxscore_endpoints)} BoxScore endpoints:\n")
#     print("=" * 60)
#     
#     # Configuration
#     SLEEP_BETWEEN_ENDPOINTS = 1.5  # Seconds to sleep between endpoint calls
#     SLEEP_BETWEEN_GAME_IDS = 0.5   # Seconds to sleep between game_id attempts
#     
#     # Known problematic endpoints that hang or timeout
#     problematic_endpoints = {"BoxScoreHustleV2"}  # Add more as discovered
#     
#     # Process each BoxScore endpoint
#     for i, (endpoint_name, endpoint_class) in enumerate(sorted(boxscore_endpoints.items()), 1):
#         print(f"\n{i}. {endpoint_name}")
#         print("-" * 60)
#         
#         # Skip known problematic endpoints
#         if endpoint_name in problematic_endpoints:
#             print(f"   ⚠️  Skipping (known to hang/timeout)")
#             # Still sleep to maintain rate limiting
#             if i < len(boxscore_endpoints):
#                 time.sleep(SLEEP_BETWEEN_ENDPOINTS)
#             continue
#         
#         # Try each game_id until one works
#         columns = None
#         error_msg = None
#         timed_out = False
#         for game_id_idx, game_id in enumerate(game_ids):
#             columns, error_msg = get_endpoint_columns(endpoint_class, endpoint_name, game_id)
#             if columns:
#                 break  # Success, no need to try other game_ids
#             # If we got a timeout, don't try other game_ids
#             if error_msg and "timed out" in error_msg.lower():
#                 timed_out = True
#                 break
#             # Sleep between game_id attempts (except after the last one)
#             if game_id_idx < len(game_ids) - 1:
#                 time.sleep(SLEEP_BETWEEN_GAME_IDS)
#         
#         if columns:
#             print(f"   ✅ Found {len(columns)} unique columns:")
#             for col in columns:
#                 print(f"   - {col}")
#         else:
#             print(f"   ❌ Could not retrieve columns")
#             if error_msg:
#                 print(f"   Error: {error_msg}")
#             if timed_out:
#                 print(f"   💡 Tip: This endpoint may be slow or unavailable. Consider skipping.")
#         
#         # Sleep between endpoints to avoid rate limiting
#         if i < len(boxscore_endpoints):
#             time.sleep(SLEEP_BETWEEN_ENDPOINTS)
#     
#     print("\n" + "=" * 60)
#     print("✅ Complete")


def find_player_id(player_name: str, season: str = "2025-26"):
    """Find a player's ID by name"""
    try:
        from nba_api.stats.endpoints import CommonAllPlayers
        
        players = CommonAllPlayers(
            season=season,
            is_only_current_season=1
        )
        players_df = players.get_data_frames()[0]
        
        # Search for player (case insensitive, partial match)
        player_name_lower = player_name.lower()
        matches = players_df[
            players_df['DISPLAY_FIRST_LAST'].str.lower().str.contains(player_name_lower, na=False)
        ]
        
        if len(matches) > 0:
            player = matches.iloc[0]
            return {
                'player_id': int(player['PERSON_ID']),
                'full_name': player['DISPLAY_FIRST_LAST'],
                'team_id': int(player['TEAM_ID']) if pd.notna(player['TEAM_ID']) else None
            }
        return None
    except Exception as e:
        print(f"Error finding player: {e}")
        return None


def main():
    """Get potential assists for LeBron James - last 10 games"""
    try:
        from nba_api.stats.endpoints import LeagueDashPtStats
    except ImportError:
        print("Error: Could not import LeagueDashPtStats")
        sys.exit(1)
    
    print("🏀 LeBron James - Potential Assists (Game-by-Game, Last 10 Games)")
    print("=" * 60)
    
    # Find LeBron James
    print("\n🔍 Finding LeBron James...")
    player_info = find_player_id("LeBron James", season="2025-26")
    
    if not player_info:
        print("❌ Could not find LeBron James. Trying alternative search...")
        # Try with "James" only and filter to LeBron
        from nba_api.stats.endpoints import CommonAllPlayers
        players = CommonAllPlayers(season="2025-26", is_only_current_season=1)
        players_df = players.get_data_frames()[0]
        lebron = players_df[players_df['DISPLAY_FIRST_LAST'].str.contains('LeBron', case=False, na=False)]
        if len(lebron) > 0:
            player_info = {
                'player_id': int(lebron.iloc[0]['PERSON_ID']),
                'full_name': lebron.iloc[0]['DISPLAY_FIRST_LAST'],
                'team_id': int(lebron.iloc[0]['TEAM_ID']) if pd.notna(lebron.iloc[0]['TEAM_ID']) else None
            }
    
    if not player_info:
        print("❌ Could not find LeBron James. Exiting.")
        sys.exit(1)
    
    print(f"✅ Found: {player_info['full_name']} (Player ID: {player_info['player_id']})")
    if player_info['team_id']:
        print(f"   Team ID: {player_info['team_id']}")
    
    # Get LeBron's game dates from box scores
    print("\n🔍 Finding LeBron's recent games...")
    try:
        from nba_api.stats.endpoints import LeagueGameLog, BoxScoreTraditionalV3
        
        # Get all games for the season
        lg = LeagueGameLog(season="2025-26", season_type_all_star="Regular Season")
        games_df = lg.get_data_frames()[0]
        
        # Get Lakers games (LeBron's team)
        lakers_team_id = 1610612747  # Los Angeles Lakers
        lakers_games = games_df[games_df['TEAM_ID'] == lakers_team_id].copy()
        
        if len(lakers_games) == 0:
            print("⚠️  Could not find Lakers games. Trying to get LeBron's games from box scores...")
            # Alternative: get recent game_ids and check box scores
            recent_game_ids = games_df.sort_values('GAME_DATE', ascending=False)['GAME_ID'].unique()[:20]
            lebron_game_dates = []
            
            for game_id in recent_game_ids:
                try:
                    boxscore = BoxScoreTraditionalV3(game_id=str(game_id))
                    players_df = boxscore.get_data_frames()[0]
                    if player_info['player_id'] in players_df['PERSON_ID'].values:
                        game_info = games_df[games_df['GAME_ID'] == game_id].iloc[0]
                        lebron_game_dates.append({
                            'game_id': str(game_id),
                            'game_date': game_info.get('GAME_DATE'),
                            'matchup': game_info.get('MATCHUP', 'Unknown')
                        })
                    time.sleep(0.5)  # Rate limiting
                    if len(lebron_game_dates) >= 10:
                        break
                except:
                    continue
        else:
            # Sort by date, most recent first
            lakers_games['GAME_DATE_DT'] = pd.to_datetime(lakers_games['GAME_DATE'], errors='coerce')
            lakers_games = lakers_games.sort_values('GAME_DATE_DT', ascending=False)
            
            # Get last 10 unique game dates
            lebron_game_dates = []
            seen_dates = set()
            
            for _, game_row in lakers_games.head(30).iterrows():  # Check more games to account for missed games
                game_date = game_row.get('GAME_DATE')
                game_id = str(game_row.get('GAME_ID', ''))
                
                if game_date and game_date not in seen_dates:
                    seen_dates.add(game_date)
                    lebron_game_dates.append({
                        'game_id': game_id,
                        'game_date': game_date,
                        'matchup': game_row.get('MATCHUP', 'Unknown')
                    })
                    
                    if len(lebron_game_dates) >= 10:
                        break
        
        if len(lebron_game_dates) == 0:
            print("❌ Could not find LeBron's games. Exiting.")
            sys.exit(1)
        
        print(f"✅ Found {len(lebron_game_dates)} recent games")
        
        print("\n" + "=" * 60)
        print("\n📊 LeBron James - Potential Assists (Game-by-Game)")
        print("=" * 60)
        
        # Get potential assists for each game
        game_stats = []
        
        for idx, game_info in enumerate(lebron_game_dates, 1):
            game_date = game_info['game_date']
            game_id = game_info['game_id']
            matchup = game_info['matchup']
            
            # Convert date format
            date_formatted = None
            if game_date:
                try:
                    if '-' in str(game_date):
                        date_parts = str(game_date).split('-')
                        if len(date_parts) == 3:
                            date_formatted = f"{date_parts[1]}/{date_parts[2]}/{date_parts[0]}"
                    else:
                        date_formatted = str(game_date)
                except:
                    date_formatted = str(game_date)
            
            print(f"\n[{idx}/{len(lebron_game_dates)}] Game: {game_id} ({game_date}) - {matchup}")
            
            try:
                # Get passing stats for this date
                endpoint = LeagueDashPtStats(
                    pt_measure_type="Passing",
                    season="2025-26",
                    season_type_all_star="Regular Season",
                    player_or_team="Player",
                    date_from_nullable=date_formatted,
                    date_to_nullable=date_formatted
                )
                
                dataframes = endpoint.get_data_frames()
                
                if len(dataframes) > 0:
                    df = dataframes[0]
                    
                    # Find LeBron in the results
                    player_id_col = None
                    for col in ['PLAYER_ID', 'PERSON_ID', 'player_id', 'person_id']:
                        if col in df.columns:
                            player_id_col = col
                            break
                    
                    lebron_row = None
                    if player_id_col:
                        lebron_rows = df[df[player_id_col] == player_info['player_id']]
                        if len(lebron_rows) > 0:
                            lebron_row = lebron_rows.iloc[0]
                    
                    if lebron_row is not None and 'POTENTIAL_AST' in lebron_row.index:
                        potential_ast = lebron_row['POTENTIAL_AST']
                        assists = lebron_row.get('AST', 'N/A')
                        passes_made = lebron_row.get('PASSES_MADE', 'N/A')
                        
                        game_stats.append({
                            'game': idx,
                            'game_id': game_id,
                            'date': game_date,
                            'matchup': matchup,
                            'potential_ast': potential_ast,
                            'assists': assists,
                            'passes_made': passes_made
                        })
                        
                        print(f"   ✅ Potential Assists: {potential_ast}")
                        print(f"      Actual Assists: {assists}")
                        print(f"      Passes Made: {passes_made}")
                    else:
                        print(f"   ⚠️  LeBron not found in this game's data")
                        game_stats.append({
                            'game': idx,
                            'game_id': game_id,
                            'date': game_date,
                            'matchup': matchup,
                            'potential_ast': None,
                            'assists': None,
                            'passes_made': None
                        })
                
                # Rate limiting
                if idx < len(lebron_game_dates):
                    time.sleep(1.5)
                    
            except Exception as e:
                print(f"   ❌ Error: {type(e).__name__}: {str(e)[:80]}")
                game_stats.append({
                    'game': idx,
                    'game_id': game_id,
                    'date': game_date,
                    'matchup': matchup,
                    'potential_ast': None,
                    'assists': None,
                    'passes_made': None
                })
        
        # Summary table
        print("\n" + "=" * 60)
        print("\n📊 Summary Table - LeBron James Potential Assists (Last 10 Games)")
        print("=" * 60)
        
        if game_stats:
            summary_df = pd.DataFrame(game_stats)
            # Reorder columns
            display_cols = ['game', 'date', 'matchup', 'potential_ast', 'assists', 'passes_made']
            display_df = summary_df[[col for col in display_cols if col in summary_df.columns]]
            print(display_df.to_string(index=False))
            
            # Calculate totals and averages
            valid_stats = [g for g in game_stats if g['potential_ast'] is not None]
            if valid_stats:
                total_potential_ast = sum(g['potential_ast'] for g in valid_stats)
                avg_potential_ast = total_potential_ast / len(valid_stats)
                total_assists = sum(g['assists'] for g in valid_stats if g['assists'] != 'N/A' and g['assists'] is not None)
                
                print(f"\n📈 Summary Statistics:")
                print(f"   Games with data: {len(valid_stats)}/{len(game_stats)}")
                print(f"   Total Potential Assists: {total_potential_ast}")
                print(f"   Average Potential Assists per game: {avg_potential_ast:.2f}")
                if total_assists:
                    print(f"   Total Actual Assists: {total_assists}")
        
        print("\n" + "=" * 60)
        print("✅ Complete")
        
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
