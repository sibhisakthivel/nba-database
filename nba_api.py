"""
Discover and print all NBA API endpoint names.

Note: This file is named nba_api.py which can conflict with the nba_api package.
We remove the current directory from sys.path before importing to avoid conflicts.
"""

import inspect
import sys
import time
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


def main():
    """Main execution"""
    print("🏀 NBA API BoxScore Endpoints - Column Names")
    print("=" * 60)
    
    # Get sample game_ids for testing
    print("\n📋 Getting sample game_ids...")
    game_ids = get_sample_game_ids(count=3)
    if not game_ids:
        print("❌ Could not get sample game_ids. Exiting.")
        sys.exit(1)
    print(f"✅ Using game_ids: {', '.join(game_ids)}\n")
    
    # Discover all endpoints
    all_endpoints = discover_all_endpoints()
    
    # Filter to BoxScore endpoints
    boxscore_endpoints = {
        name: endpoint_class 
        for name, endpoint_class in all_endpoints.items() 
        if name.startswith("BoxScore")
    }
    
    print(f"Found {len(boxscore_endpoints)} BoxScore endpoints:\n")
    print("=" * 60)
    
    # Configuration
    SLEEP_BETWEEN_ENDPOINTS = 1.5  # Seconds to sleep between endpoint calls
    SLEEP_BETWEEN_GAME_IDS = 0.5   # Seconds to sleep between game_id attempts
    
    # Known problematic endpoints that hang or timeout
    problematic_endpoints = {"BoxScoreHustleV2"}  # Add more as discovered
    
    # Process each BoxScore endpoint
    for i, (endpoint_name, endpoint_class) in enumerate(sorted(boxscore_endpoints.items()), 1):
        print(f"\n{i}. {endpoint_name}")
        print("-" * 60)
        
        # Skip known problematic endpoints
        if endpoint_name in problematic_endpoints:
            print(f"   ⚠️  Skipping (known to hang/timeout)")
            # Still sleep to maintain rate limiting
            if i < len(boxscore_endpoints):
                time.sleep(SLEEP_BETWEEN_ENDPOINTS)
            continue
        
        # Try each game_id until one works
        columns = None
        error_msg = None
        timed_out = False
        for game_id_idx, game_id in enumerate(game_ids):
            columns, error_msg = get_endpoint_columns(endpoint_class, endpoint_name, game_id)
            if columns:
                break  # Success, no need to try other game_ids
            # If we got a timeout, don't try other game_ids
            if error_msg and "timed out" in error_msg.lower():
                timed_out = True
                break
            # Sleep between game_id attempts (except after the last one)
            if game_id_idx < len(game_ids) - 1:
                time.sleep(SLEEP_BETWEEN_GAME_IDS)
        
        if columns:
            print(f"   ✅ Found {len(columns)} unique columns:")
            for col in columns:
                print(f"   - {col}")
        else:
            print(f"   ❌ Could not retrieve columns")
            if error_msg:
                print(f"   Error: {error_msg}")
            if timed_out:
                print(f"   💡 Tip: This endpoint may be slow or unavailable. Consider skipping.")
        
        # Sleep between endpoints to avoid rate limiting
        if i < len(boxscore_endpoints):
            time.sleep(SLEEP_BETWEEN_ENDPOINTS)
    
    print("\n" + "=" * 60)
    print("✅ Complete")


if __name__ == "__main__":
    main()
