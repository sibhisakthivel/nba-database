from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db.connection import get_engine


BASE_RAW_DIR = Path("data/raw/league_game_log")
BASE_RAW_DIR.mkdir(parents=True, exist_ok=True)


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.lower()
    )
    return df


def ingest_league_game_log(season: str, season_type: str = "Regular Season") -> None:
    """
    Ingest LeagueGameLog data for a full season.

    - Writes season-scoped raw CSV snapshot
    - Inserts rows into raw.league_game_log
    - Safe to re-run (primary key prevents duplicates)
    """

    engine = get_engine()

    # 1. Pull data from NBA API
    lg = LeagueGameLog(
        season=season,
        season_type_all_star=season_type
    )

    lg_df = lg.get_data_frames()[0]

    # 2. Normalize column names
    lg_df = _snake_case_columns(lg_df)
    
    # Fix column name mismatches (snake_case creates fg3_m/fg3_a but SQL expects fg3m/fg3a)
    lg_df = lg_df.rename(columns={
        'fg3_m': 'fg3m',
        'fg3_a': 'fg3a'
    })

    # 3. Save raw CSV snapshot (season-scoped)
    csv_path = (
        BASE_RAW_DIR
        / f"league_game_log_{season}_{season_type.replace(' ', '_').lower()}.csv"
    )

    lg_df.to_csv(csv_path, index=False)

    insert_sql = text("""
        INSERT INTO raw.league_game_log (
            season_id,
            game_id,
            team_id,
            team_abbreviation,
            team_name,
            game_date,
            matchup,
            wl,
            min,
            fgm,
            fga,
            fg_pct,
            fg3m,
            fg3a,
            fg3_pct,
            ftm,
            fta,
            ft_pct,
            oreb,
            dreb,
            reb,
            ast,
            stl,
            blk,
            tov,
            pf,
            pts,
            plus_minus,
            video_available
        )
        VALUES (
            :season_id,
            :game_id,
            :team_id,
            :team_abbreviation,
            :team_name,
            :game_date,
            :matchup,
            :wl,
            :min,
            :fgm,
            :fga,
            :fg_pct,
            :fg3m,
            :fg3a,
            :fg3_pct,
            :ftm,
            :fta,
            :ft_pct,
            :oreb,
            :dreb,
            :reb,
            :ast,
            :stl,
            :blk,
            :tov,
            :pf,
            :pts,
            :plus_minus,
            :video_available
        )
    """)

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, row in lg_df.iterrows():
            try:
                conn.execute(insert_sql, row.to_dict())
                inserted += 1
            except IntegrityError:
                skipped += 1

    print(
        f"[LeagueGameLog] "
        f"season={season} type={season_type} "
        f"inserted={inserted} skipped={skipped}"
    )
