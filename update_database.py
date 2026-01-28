import time
from typing import Set

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog
from sqlalchemy import text

from db.connection import get_engine
from ingestion.box_score_traditional_v3 import ingest_box_score_traditional_v3
from ingestion.league_game_log import ingest_league_game_log
from ingestion.potential_ast import (
    get_all_game_ids_from_api as get_all_game_ids_for_potential_ast,
    get_ingested_game_ids_from_db as get_ingested_potential_ast_game_ids,
    get_all_game_dates_from_api,
    get_ingested_game_dates_from_db,
    get_game_ids_with_dates,
    ingest_potential_ast_for_game_ids
)


# -----------------------------
# CONFIG
# -----------------------------

SEASON = "2025-26"
SEASON_TYPE = "Regular Season"

# Sleep between NBA API calls (seconds)
SLEEP_SECONDS = 1.5


# -----------------------------
# DISCOVERY FUNCTIONS
# -----------------------------

def get_all_game_ids_from_api(season: str, season_type: str) -> Set[str]:
    """
    Fetch all game_ids available from the NBA API for a given season.
    """
    lg = LeagueGameLog(
        season=season,
        season_type_all_star=season_type
    )

    games_df = lg.get_data_frames()[0]
    return set(games_df["GAME_ID"].unique())


def get_ingested_game_ids_from_db() -> Set[str]:
    """
    Fetch distinct game_ids already ingested into raw.box_score_traditional_v3.
    """
    engine = get_engine()

    query = """
        SELECT DISTINCT game_id
        FROM raw.box_score_traditional_v3
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return {row[0] for row in result}


# -----------------------------
# ORCHESTRATION
# -----------------------------

def update_box_score_traditional_v3():
    """
    One-click updater:
    - finds missing games
    - ingests them one-by-one
    - sleeps between API calls
    """

    print("🔍 Discovering games from NBA API...")
    api_game_ids = get_all_game_ids_from_api(SEASON, SEASON_TYPE)

    print("📦 Checking existing database records...")
    db_game_ids = get_ingested_game_ids_from_db()

    missing_game_ids = sorted(api_game_ids - db_game_ids)

    print(f"🏀 Season: {SEASON}")
    print(f"📊 Games in API: {len(api_game_ids)}")
    print(f"✅ Games already ingested: {len(db_game_ids)}")
    print(f"⏳ Games to ingest: {len(missing_game_ids)}")

    for i, game_id in enumerate(missing_game_ids, start=1):
        print(f"\n➡️  [{i}/{len(missing_game_ids)}] Ingesting game_id={game_id}")

        try:
            ingest_box_score_traditional_v3(game_id, SEASON)
        except Exception as e:
            # Fail loudly but continue
            print(f"❌ Error ingesting game_id={game_id}: {e}")

        # Rate limiting
        print(f"🕒 Sleeping {SLEEP_SECONDS}s...")
        time.sleep(SLEEP_SECONDS)

    print("\n✅ Update complete.")


def update_league_game_log():
    """
    One-click updater for league game log:
    - ingests full season game log data
    - safe to re-run (primary key prevents duplicates)
    """
    print(f"\n📋 Ingesting LeagueGameLog for {SEASON} ({SEASON_TYPE})...")
    
    try:
        ingest_league_game_log(SEASON, SEASON_TYPE)
        print("✅ LeagueGameLog update complete.")
    except Exception as e:
        print(f"❌ Error ingesting LeagueGameLog: {e}")
        raise


def update_potential_ast():
    """
    One-click updater for potential assists:
    - finds missing games (compared to box_score_traditional_v3)
    - ingests them grouped by date
    - sleeps between API calls
    
    Note: Requires box_score_traditional_v3 to be populated first
    since it uses that table to match players to game_ids.
    
    Uses ON CONFLICT DO NOTHING so safe to re-run on all games.
    """
    print("\n🔍 Discovering games for potential assists...")
    api_game_ids = get_all_game_ids_for_potential_ast(SEASON, SEASON_TYPE)
    
    print("📦 Checking existing potential_ast records...")
    db_game_ids = get_ingested_potential_ast_game_ids()
    
    # Find completely missing games
    missing_game_ids = sorted(api_game_ids - db_game_ids)
    
    print(f"🏀 Season: {SEASON}")
    print(f"📊 Games in API: {len(api_game_ids)}")
    print(f"✅ Games already ingested: {len(db_game_ids)}")
    print(f"⏳ Games to ingest: {len(missing_game_ids)}")
    
    if len(missing_game_ids) == 0:
        print("✅ No new games to ingest.")
        return
    
    # Process missing games (grouped by date internally)
    # Note: Uses ON CONFLICT DO NOTHING so safe to re-run
    # The "Inserted" count shows actual new inserts, "Skipped" shows conflicts (already exists)
    try:
        ingest_potential_ast_for_game_ids(
            game_ids=missing_game_ids,
            season=SEASON,
            season_type=SEASON_TYPE
        )
        print("\n✅ Potential assists update complete.")
    except Exception as e:
        print(f"\n❌ Error ingesting potential assists: {e}")
        import traceback
        traceback.print_exc()
        raise


def update_potential_ast_all():
    """
    Re-process ALL games for potential assists (ignores what's already in DB).
    Useful for ensuring no rows were skipped or fixing data issues.
    
    Uses ON CONFLICT DO NOTHING so existing rows won't be duplicated,
    but will attempt to insert any missing rows.
    """
    print("\n🔄 Re-processing ALL games for potential assists...")
    api_game_ids = get_all_game_ids_for_potential_ast(SEASON, SEASON_TYPE)
    
    print(f"🏀 Season: {SEASON}")
    print(f"📊 Total games to process: {len(api_game_ids)}")
    print("⚠️  Processing ALL games (will skip existing rows)")
    
    # Process ALL games (grouped by date internally)
    # Note: Uses ON CONFLICT DO NOTHING so safe to re-run
    # The "Inserted" count shows actual new inserts, "Skipped" shows conflicts (already exists)
    try:
        ingest_potential_ast_for_game_ids(
            game_ids=sorted(api_game_ids),
            season=SEASON,
            season_type=SEASON_TYPE
        )
        print("\n✅ Potential assists full re-process complete.")
    except Exception as e:
        print(f"\n❌ Error ingesting potential assists: {e}")
        import traceback
        traceback.print_exc()
        raise


def update_potential_ast_daily():
    """
    Daily updater for potential assists:
    - Only processes NEW game dates (dates not already in database)
    - More efficient for daily updates since it skips dates already processed
    - Uses ON CONFLICT DO NOTHING so safe to re-run
    
    Note: Requires box_score_traditional_v3 to be populated first
    since it uses that table to match players to game_ids.
    """
    print("\n🔍 Discovering NEW game dates for potential assists...")
    api_dates = get_all_game_dates_from_api(SEASON, SEASON_TYPE)
    
    print("📦 Checking existing potential_ast dates...")
    db_dates = get_ingested_game_dates_from_db()
    
    # Find new dates
    new_dates = sorted(api_dates - db_dates)
    
    print(f"🏀 Season: {SEASON}")
    print(f"📊 Dates in API: {len(api_dates)}")
    print(f"✅ Dates already ingested: {len(db_dates)}")
    print(f"⏳ New dates to ingest: {len(new_dates)}")
    
    if len(new_dates) == 0:
        print("✅ No new dates to ingest.")
        return
    
    # Get all game_ids for the new dates
    games_df = get_game_ids_with_dates(SEASON, SEASON_TYPE)
    
    # Convert dates to match format (handle both string and date object formats)
    new_dates_set = set(new_dates)
    games_df['date_str'] = games_df['GAME_DATE'].apply(
        lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)
    )
    
    # Filter to new dates
    new_games_df = games_df[games_df['date_str'].isin(new_dates_set)]
    new_game_ids = sorted(new_games_df['GAME_ID'].unique().tolist())
    
    print(f"📋 Games in new dates: {len(new_game_ids)}")
    
    # Process new dates (grouped by date internally)
    try:
        ingest_potential_ast_for_game_ids(
            game_ids=new_game_ids,
            season=SEASON,
            season_type=SEASON_TYPE
        )
        print("\n✅ Potential assists daily update complete.")
    except Exception as e:
        print(f"\n❌ Error ingesting potential assists: {e}")
        import traceback
        traceback.print_exc()
        raise


def fix_missing_potential_ast():
    """
    Helper function to re-process ALL missing games.
    Useful for catching up on missed data or fixing issues.
    """
    print("\n🔧 Re-processing all missing potential_ast data...")
    update_potential_ast()


# -----------------------------
# ENTRY POINT
# -----------------------------

if __name__ == "__main__":
    update_league_game_log()
    update_box_score_traditional_v3()
    # Use update_potential_ast_all() to re-process ALL games/dates
    # Use update_potential_ast() to only process missing games
    # Use update_potential_ast_daily() for daily updates (new dates only)
    update_potential_ast_daily()
