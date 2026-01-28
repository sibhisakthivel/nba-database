from pathlib import Path
import math

import numpy as np
import pandas as pd
from nba_api.stats.endpoints import BoxScorePlayerTrackV3
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db.connection import get_engine


RAW_CSV_DIR = Path("data/raw/box_score_player_track_v3")
RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)


def _clean_nan_values(value):
    """Convert NaN/None values to None (NULL for database)"""
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (np.floating, np.integer)) and (np.isnan(value) or np.isinf(value)):
        return None
    return value


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.lower()
    )
    return df


def ingest_box_score_player_track_v3(game_id: str, season: str) -> None:
    """
    Ingest player tracking box score data for a single NBA game (BoxScorePlayerTrackV3).

    - Writes raw CSV snapshot
    - Inserts rows one-by-one into raw.box_score_player_track_v3
    - Safe to re-run (primary key prevents duplicates)
    """

    engine = get_engine()

    # 1. Pull data
    endpoint = BoxScorePlayerTrackV3(game_id=game_id)
    players_df = endpoint.get_data_frames()[0]

    # 2. Normalize column names
    players_df = _snake_case_columns(players_df)

    # 3. Save raw CSV snapshot in season subfolder
    season_dir = RAW_CSV_DIR / season
    season_dir.mkdir(parents=True, exist_ok=True)
    players_df.to_csv(season_dir / f"{game_id}.csv", index=False)

    insert_sql = text("""
        INSERT INTO raw.box_score_player_track_v3 (
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
            assists,
            free_throw_assists,
            secondary_assists,
            passes,
            touches,
            field_goal_percentage,
            contested_field_goal_percentage,
            uncontested_field_goals_percentage,
            defended_at_rim_field_goal_percentage,
            contested_field_goals_made,
            contested_field_goals_attempted,
            uncontested_field_goals_made,
            uncontested_field_goals_attempted,
            defended_at_rim_field_goals_made,
            defended_at_rim_field_goals_attempted,
            rebound_chances_offensive,
            rebound_chances_defensive,
            rebound_chances_total,
            distance,
            speed
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
            :assists,
            :free_throw_assists,
            :secondary_assists,
            :passes,
            :touches,
            :field_goal_percentage,
            :contested_field_goal_percentage,
            :uncontested_field_goals_percentage,
            :defended_at_rim_field_goal_percentage,
            :contested_field_goals_made,
            :contested_field_goals_attempted,
            :uncontested_field_goals_made,
            :uncontested_field_goals_attempted,
            :defended_at_rim_field_goals_made,
            :defended_at_rim_field_goals_attempted,
            :rebound_chances_offensive,
            :rebound_chances_defensive,
            :rebound_chances_total,
            :distance,
            :speed
        )
        ON CONFLICT (game_id, person_id) DO NOTHING;
    """)

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, row in players_df.iterrows():
            try:
                # Clean NaN values for numeric columns
                row_data = row.to_dict()
                numeric_columns = [
                    'assists', 'free_throw_assists', 'secondary_assists',
                    'passes', 'touches',
                    'field_goal_percentage', 'contested_field_goal_percentage',
                    'uncontested_field_goals_percentage', 'defended_at_rim_field_goal_percentage',
                    'contested_field_goals_made', 'contested_field_goals_attempted',
                    'uncontested_field_goals_made', 'uncontested_field_goals_attempted',
                    'defended_at_rim_field_goals_made', 'defended_at_rim_field_goals_attempted',
                    'rebound_chances_offensive', 'rebound_chances_defensive', 'rebound_chances_total',
                    'distance', 'speed'
                ]
                for col in numeric_columns:
                    if col in row_data:
                        row_data[col] = _clean_nan_values(row_data[col])
                
                conn.execute(insert_sql, row_data)
                inserted += 1
            except IntegrityError:
                skipped += 1

    print(f"[BoxScorePlayerTrackV3] game_id={game_id} inserted={inserted} skipped={skipped}")
