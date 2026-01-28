CREATE TABLE IF NOT EXISTS raw.box_score_traditional_v3 (
    -- Identifiers
    game_id TEXT NOT NULL,
    team_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,

    -- Team info
    team_city TEXT,
    team_name TEXT,
    team_tricode TEXT,
    team_slug TEXT,

    -- Player info
    first_name TEXT,
    family_name TEXT,
    name_i TEXT,
    player_slug TEXT,
    position TEXT,
    comment TEXT,
    jersey_num TEXT,

    -- Playing time
    minutes TEXT,

    -- Shooting
    field_goals_made INTEGER,
    field_goals_attempted INTEGER,
    field_goals_percentage DOUBLE PRECISION,

    three_pointers_made INTEGER,
    three_pointers_attempted INTEGER,
    three_pointers_percentage DOUBLE PRECISION,

    free_throws_made INTEGER,
    free_throws_attempted INTEGER,
    free_throws_percentage DOUBLE PRECISION,

    -- Rebounding
    rebounds_offensive INTEGER,
    rebounds_defensive INTEGER,
    rebounds_total INTEGER,

    -- Other stats
    assists INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fouls_personal INTEGER,
    points INTEGER,
    plus_minus_points DOUBLE PRECISION,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_raw_box_score_traditional_v3
        PRIMARY KEY (season_id, game_id, person_id)
);
