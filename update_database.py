import time
from typing import Set

from nba_api.stats.endpoints import LeagueGameLog
from sqlalchemy import text

from db.connection import get_engine
from ingestion.box_score_traditional_v3 import ingest_box_score_traditional_v3
from ingestion.league_game_log import ingest_league_game_log


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


# -----------------------------
# ENTRY POINT
# -----------------------------

if __name__ == "__main__":
    update_league_game_log()
    update_box_score_traditional_v3()
