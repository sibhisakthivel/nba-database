CREATE TABLE IF NOT EXISTS raw.league_game_log (
    -- Identifiers
    season_id TEXT NOT NULL,
    game_id TEXT NOT NULL,
    team_id INTEGER NOT NULL,

    -- Team info
    team_abbreviation TEXT,
    team_name TEXT,

    -- Game metadata
    game_date DATE,
    matchup TEXT,
    wl TEXT,

    -- Minutes
    min INTEGER,

    -- Shooting
    fgm INTEGER,
    fga INTEGER,
    fg_pct DOUBLE PRECISION,

    fg3m INTEGER,
    fg3a INTEGER,
    fg3_pct DOUBLE PRECISION,

    ftm INTEGER,
    fta INTEGER,
    ft_pct DOUBLE PRECISION,

    -- Rebounding
    oreb INTEGER,
    dreb INTEGER,
    reb INTEGER,

    -- Other stats
    ast INTEGER,
    stl INTEGER,
    blk INTEGER,
    tov INTEGER,
    pf INTEGER,
    pts INTEGER,
    plus_minus INTEGER,

    -- Media / flags
    video_available INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_raw_league_game_log
        PRIMARY KEY (game_id, team_id)
);
