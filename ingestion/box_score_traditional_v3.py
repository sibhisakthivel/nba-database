from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db.connection import get_engine


RAW_CSV_DIR = Path("data/raw/box_score_traditional_v3")
RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.lower()
    )
    return df


def ingest_box_score_traditional_v3(game_id: str, season: str) -> None:
    """
    Ingest player box score data for a single NBA game (BoxScoreTraditionalV3).

    - Writes raw CSV snapshot
    - Inserts rows one-by-one into raw.box_score_traditional_v3
    - Safe to re-run (primary key prevents duplicates)
    """

    engine = get_engine()

    # 1. Pull data
    endpoint = BoxScoreTraditionalV3(game_id=game_id)
    players_df = endpoint.get_data_frames()[0]

    # 2. Normalize column names
    players_df = _snake_case_columns(players_df)

    # 3. Save raw CSV snapshot in season subfolder
    season_dir = RAW_CSV_DIR / season
    season_dir.mkdir(parents=True, exist_ok=True)
    players_df.to_csv(season_dir / f"{game_id}.csv", index=False)

    insert_sql = text("""
        INSERT INTO raw.box_score_traditional_v3 (
            game_id,
            team_id,
            person_id,
            team_city,
            team_name,
            team_tricode,
            team_slug,
            first_name,
            family_name,
            name_i,
            player_slug,
            position,
            comment,
            jersey_num,
            minutes,
            field_goals_made,
            field_goals_attempted,
            field_goals_percentage,
            three_pointers_made,
            three_pointers_attempted,
            three_pointers_percentage,
            free_throws_made,
            free_throws_attempted,
            free_throws_percentage,
            rebounds_offensive,
            rebounds_defensive,
            rebounds_total,
            assists,
            steals,
            blocks,
            turnovers,
            fouls_personal,
            points,
            plus_minus_points
        )
        VALUES (
            :game_id,
            :team_id,
            :person_id,
            :team_city,
            :team_name,
            :team_tricode,
            :team_slug,
            :first_name,
            :family_name,
            :name_i,
            :player_slug,
            :position,
            :comment,
            :jersey_num,
            :minutes,
            :field_goals_made,
            :field_goals_attempted,
            :field_goals_percentage,
            :three_pointers_made,
            :three_pointers_attempted,
            :three_pointers_percentage,
            :free_throws_made,
            :free_throws_attempted,
            :free_throws_percentage,
            :rebounds_offensive,
            :rebounds_defensive,
            :rebounds_total,
            :assists,
            :steals,
            :blocks,
            :turnovers,
            :fouls_personal,
            :points,
            :plus_minus_points
        )
    """)

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, row in players_df.iterrows():
            try:
                conn.execute(insert_sql, row.to_dict())
                inserted += 1
            except IntegrityError:
                skipped += 1

    print(f"[BoxScoreTraditionalV3] game_id={game_id} inserted={inserted} skipped={skipped}")
