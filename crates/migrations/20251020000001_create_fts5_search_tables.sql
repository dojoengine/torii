-- Create FTS5 virtual tables for full-text search
-- These tables enable fast, ranked search across text fields

-- ========================================
-- Achievements FTS5 Table
-- ========================================
CREATE VIRTUAL TABLE IF NOT EXISTS achievements_fts USING fts5(
    id UNINDEXED,           -- Store but don't index the ID (used for joins)
    title,                  -- Searchable: achievement title
    description,            -- Searchable: achievement description
    group_name,             -- Searchable: achievement group
    world_address UNINDEXED,-- Filter field (not searchable)
    namespace UNINDEXED,    -- Filter field (not searchable)
    tokenize='porter unicode61 remove_diacritics 2'  -- Advanced tokenization
);

-- Triggers to keep FTS5 table in sync with achievements table
CREATE TRIGGER IF NOT EXISTS achievements_fts_insert AFTER INSERT ON achievements BEGIN
    INSERT INTO achievements_fts(id, title, description, group_name, world_address, namespace)
    VALUES (new.id, new.title, new.description, new.group_name, new.world_address, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS achievements_fts_update AFTER UPDATE ON achievements BEGIN
    UPDATE achievements_fts 
    SET title = new.title,
        description = new.description,
        group_name = new.group_name,
        world_address = new.world_address,
        namespace = new.namespace
    WHERE id = old.id;
END;

CREATE TRIGGER IF NOT EXISTS achievements_fts_delete AFTER DELETE ON achievements BEGIN
    DELETE FROM achievements_fts WHERE id = old.id;
END;

-- ========================================
-- Controllers FTS5 Table
-- ========================================
CREATE VIRTUAL TABLE IF NOT EXISTS controllers_fts USING fts5(
    id UNINDEXED,           -- Store but don't index the ID
    username,               -- Searchable: controller username
    address UNINDEXED,      -- Store for retrieval but don't search (not useful for FTS)
    tokenize='unicode61 remove_diacritics 2'
);

-- Triggers to keep FTS5 table in sync with controllers table
CREATE TRIGGER IF NOT EXISTS controllers_fts_insert AFTER INSERT ON controllers BEGIN
    INSERT INTO controllers_fts(id, username, address)
    VALUES (new.id, new.username, new.address);
END;

CREATE TRIGGER IF NOT EXISTS controllers_fts_update AFTER UPDATE ON controllers BEGIN
    UPDATE controllers_fts 
    SET username = new.username,
        address = new.address
    WHERE id = old.id;
END;

CREATE TRIGGER IF NOT EXISTS controllers_fts_delete AFTER DELETE ON controllers BEGIN
    DELETE FROM controllers_fts WHERE id = old.id;
END;

-- ========================================
-- Token Attributes FTS5 Table
-- ========================================
CREATE VIRTUAL TABLE IF NOT EXISTS token_attributes_fts USING fts5(
    id UNINDEXED,           -- Store but don't index the ID
    token_id UNINDEXED,     -- Store for retrieval
    trait_name,             -- Searchable: NFT trait name
    trait_value,            -- Searchable: NFT trait value
    tokenize='unicode61 remove_diacritics 2'
);

-- Triggers to keep FTS5 table in sync with token_attributes table
CREATE TRIGGER IF NOT EXISTS token_attributes_fts_insert AFTER INSERT ON token_attributes BEGIN
    INSERT INTO token_attributes_fts(id, token_id, trait_name, trait_value)
    VALUES (new.id, new.token_id, new.trait_name, new.trait_value);
END;

CREATE TRIGGER IF NOT EXISTS token_attributes_fts_update AFTER UPDATE ON token_attributes BEGIN
    UPDATE token_attributes_fts 
    SET token_id = new.token_id,
        trait_name = new.trait_name,
        trait_value = new.trait_value
    WHERE id = old.id;
END;

CREATE TRIGGER IF NOT EXISTS token_attributes_fts_delete AFTER DELETE ON token_attributes BEGIN
    DELETE FROM token_attributes_fts WHERE id = old.id;
END;

-- ========================================
-- Initial Population
-- ========================================
-- Populate FTS5 tables with existing data
INSERT INTO achievements_fts(id, title, description, group_name, world_address, namespace)
SELECT id, title, description, group_name, world_address, namespace FROM achievements;

INSERT INTO controllers_fts(id, username, address)
SELECT id, username, address FROM controllers;

INSERT INTO token_attributes_fts(id, token_id, trait_name, trait_value)
SELECT id, token_id, trait_name, trait_value FROM token_attributes;

-- ========================================
-- FTS5 Configuration Notes
-- ========================================
-- Tokenizer: 'porter unicode61 remove_diacritics 2'
--   - porter: English stemming (running/runs -> run)
--   - unicode61: Unicode-aware tokenization
--   - remove_diacritics 2: Remove accents (café -> cafe)
--
-- UNINDEXED columns: Stored in FTS5 but not searchable
--   - Used for filtering (world_address, namespace)
--   - Used for retrieval (id, token_id, address)
--   - Saves index space for non-searchable data
--
-- BM25 Ranking: FTS5 provides built-in BM25 scoring via rank column
--   - Access via: SELECT *, rank FROM table_fts WHERE table_fts MATCH ?
--   - Lower rank values = better matches (more relevant)
--
-- Query Syntax Examples:
--   - Simple: "dragon"
--   - Phrase: '"dragon slayer"'
--   - Prefix: "dra*"
--   - Boolean: "dragon OR knight"
--   - Column-specific: "title:dragon"
--
-- Optimization Commands:
--   - PRAGMA optimize;  -- Run periodically for query optimization
--   - INSERT INTO table_fts(table_fts) VALUES('optimize');  -- Rebuild FTS5 index

