-- ========================================
-- Unified Search Index (FTS5)
-- ========================================
-- Single FTS5 table for all searchable content
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    entity_type UNINDEXED,   -- Type: 'achievement', 'controller', 'token_attribute', etc.
    entity_id UNINDEXED,      -- Primary key from source table
    primary_text,             -- Main searchable text (title, username, name, etc.)
    secondary_text,           -- Secondary searchable text (description, etc.)
    metadata UNINDEXED,       -- JSON metadata for filtering (world_address, namespace, etc.)
    tokenize='porter unicode61 remove_diacritics 2'
);

-- ========================================
-- Achievements Triggers
-- ========================================
CREATE TRIGGER IF NOT EXISTS search_index_achievements_insert AFTER INSERT ON achievements BEGIN
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'achievement',
        new.id,
        new.title || ' ' || COALESCE(new.group_name, ''),
        new.description,
        json_object(
            'world_address', new.world_address,
            'namespace', new.namespace,
            'group_name', new.group_name
        )
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_achievements_update AFTER UPDATE ON achievements BEGIN
    DELETE FROM search_index WHERE entity_type = 'achievement' AND entity_id = old.id;
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'achievement',
        new.id,
        new.title || ' ' || COALESCE(new.group_name, ''),
        new.description,
        json_object(
            'world_address', new.world_address,
            'namespace', new.namespace,
            'group_name', new.group_name
        )
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_achievements_delete AFTER DELETE ON achievements BEGIN
    DELETE FROM search_index WHERE entity_type = 'achievement' AND entity_id = old.id;
END;

-- ========================================
-- Controllers Triggers
-- ========================================
CREATE TRIGGER IF NOT EXISTS search_index_controllers_insert AFTER INSERT ON controllers BEGIN
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'controller',
        new.id,
        new.username,
        '',
        json_object('address', new.address)
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_controllers_update AFTER UPDATE ON controllers BEGIN
    DELETE FROM search_index WHERE entity_type = 'controller' AND entity_id = old.id;
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'controller',
        new.id,
        new.username,
        '',
        json_object('address', new.address)
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_controllers_delete AFTER DELETE ON controllers BEGIN
    DELETE FROM search_index WHERE entity_type = 'controller' AND entity_id = old.id;
END;

-- ========================================
-- Token Attributes Triggers
-- ========================================
CREATE TRIGGER IF NOT EXISTS search_index_token_attributes_insert AFTER INSERT ON token_attributes BEGIN
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'token_attribute',
        new.id,
        new.trait_name,
        new.trait_value,
        json_object('token_id', new.token_id)
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_token_attributes_update AFTER UPDATE ON token_attributes BEGIN
    DELETE FROM search_index WHERE entity_type = 'token_attribute' AND entity_id = old.id;
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'token_attribute',
        new.id,
        new.trait_name,
        new.trait_value,
        json_object('token_id', new.token_id)
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_token_attributes_delete AFTER DELETE ON token_attributes BEGIN
    DELETE FROM search_index WHERE entity_type = 'token_attribute' AND entity_id = old.id;
END;

-- ========================================
-- Tokens Triggers (ERC20 only - token_id IS NULL)
-- ========================================
CREATE TRIGGER IF NOT EXISTS search_index_tokens_insert AFTER INSERT ON tokens
WHEN new.token_id IS NULL
BEGIN
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'token',
        new.id,
        new.name || ' ' || new.symbol,
        COALESCE(new.metadata, ''),
        json_object(
            'contract_address', new.contract_address,
            'symbol', new.symbol,
            'decimals', new.decimals
        )
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_tokens_update AFTER UPDATE ON tokens
WHEN new.token_id IS NULL
BEGIN
    DELETE FROM search_index WHERE entity_type = 'token' AND entity_id = old.id;
    INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
    VALUES (
        'token',
        new.id,
        new.name || ' ' || new.symbol,
        COALESCE(new.metadata, ''),
        json_object(
            'contract_address', new.contract_address,
            'symbol', new.symbol,
            'decimals', new.decimals
        )
    );
END;

CREATE TRIGGER IF NOT EXISTS search_index_tokens_delete AFTER DELETE ON tokens
WHEN old.token_id IS NULL
BEGIN
    DELETE FROM search_index WHERE entity_type = 'token' AND entity_id = old.id;
END;

-- ========================================
-- Initial Population
-- ========================================
-- Populate unified search index with existing data
INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
SELECT 
    'achievement',
    id,
    title || ' ' || COALESCE(group_name, ''),
    description,
    json_object(
        'world_address', world_address,
        'namespace', namespace,
        'group_name', group_name
    )
FROM achievements;

INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
SELECT 
    'controller',
    id,
    username,
    '',
    json_object('address', address)
FROM controllers;

INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
SELECT 
    'token_attribute',
    id,
    trait_name,
    trait_value,
    json_object('token_id', token_id)
FROM token_attributes;

INSERT INTO search_index(entity_type, entity_id, primary_text, secondary_text, metadata)
SELECT 
    'token',
    id,
    name || ' ' || symbol,
    COALESCE(metadata, ''),
    json_object(
        'contract_address', contract_address,
        'symbol', symbol,
        'decimals', decimals
    )
FROM tokens
WHERE token_id IS NULL;

-- ========================================
-- Usage Notes
-- ========================================
-- Indexed Entity Types:
--   - achievement: Searchable by title, description, group_name
--   - controller: Searchable by username
--   - token_attribute: Searchable by trait_name, trait_value (NFT traits)
--   - token: Searchable by name, symbol (ERC20 tokens only, token_id IS NULL)
--
-- Search all types:
--   SELECT * FROM search_index WHERE search_index MATCH 'dragon' ORDER BY rank;
--
-- Search specific type:
--   SELECT * FROM search_index 
--   WHERE search_index MATCH 'USDC' AND entity_type = 'token' 
--   ORDER BY rank;
--
-- Search with metadata filter:
--   SELECT * FROM search_index 
--   WHERE search_index MATCH 'dragon' 
--   AND json_extract(metadata, '$.namespace') = 'my_game'
--   ORDER BY rank;
--
-- Search tokens by symbol:
--   SELECT * FROM search_index 
--   WHERE search_index MATCH 'ETH' AND entity_type = 'token'
--   ORDER BY rank;
--
-- Add new searchable entity type:
--   1. Create triggers for INSERT/UPDATE/DELETE with optional WHEN clause
--   2. Use consistent entity_type value
--   3. Map entity fields to primary_text/secondary_text
--   4. Store filterable data in metadata JSON
--   5. Add initial population INSERT in migration

