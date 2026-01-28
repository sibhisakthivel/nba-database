CREATE TABLE IF NOT EXISTS raw.potential_ast (
    -- Identifiers
    game_id TEXT NOT NULL,
    person_id INTEGER NOT NULL,
    game_date DATE,

    -- Game context
    gp INTEGER,
    w INTEGER,
    l INTEGER,
    min DOUBLE PRECISION,

    -- Passing statistics
    passes_made INTEGER,
    passes_received INTEGER,
    ast INTEGER,
    ft_ast INTEGER,
    secondary_ast INTEGER,
    potential_ast INTEGER,
    ast_pts_created INTEGER,
    ast_adj INTEGER,
    ast_to_pass_pct DOUBLE PRECISION,
    ast_to_pass_pct_adj DOUBLE PRECISION,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_raw_potential_ast
        PRIMARY KEY (game_id, person_id)
);
