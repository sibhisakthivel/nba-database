CREATE TABLE IF NOT EXISTS raw.box_score_player_track_v3 (
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

    -- Assists
    assists INTEGER,
    free_throw_assists INTEGER,
    secondary_assists INTEGER,

    -- Passing
    passes INTEGER,
    touches INTEGER,

    -- Shooting percentages
    field_goal_percentage DOUBLE PRECISION,
    contested_field_goal_percentage DOUBLE PRECISION,
    uncontested_field_goals_percentage DOUBLE PRECISION,
    defended_at_rim_field_goal_percentage DOUBLE PRECISION,

    -- Contested shots
    contested_field_goals_made INTEGER,
    contested_field_goals_attempted INTEGER,

    -- Uncontested shots
    uncontested_field_goals_made INTEGER,
    uncontested_field_goals_attempted INTEGER,

    -- Defended at rim
    defended_at_rim_field_goals_made INTEGER,
    defended_at_rim_field_goals_attempted INTEGER,

    -- Rebound chances
    rebound_chances_offensive INTEGER,
    rebound_chances_defensive INTEGER,
    rebound_chances_total INTEGER,

    -- Player tracking
    distance DOUBLE PRECISION,
    speed DOUBLE PRECISION,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_raw_box_score_player_track_v3
        PRIMARY KEY (game_id, person_id)
);
