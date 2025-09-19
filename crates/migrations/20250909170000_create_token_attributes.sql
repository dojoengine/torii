-- Create token_attributes table for fast NFT trait filtering
CREATE TABLE token_attributes (
    id TEXT PRIMARY KEY,
    token_id TEXT NOT NULL,
    trait_name TEXT NOT NULL,
    trait_value TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (token_id) REFERENCES tokens(id)
);

-- Indexes for fast filtering
CREATE INDEX idx_token_attributes_lookup ON token_attributes (trait_name, trait_value);
CREATE INDEX idx_token_attributes_token ON token_attributes (token_id);
CREATE INDEX idx_token_attributes_name ON token_attributes (trait_name);
CREATE INDEX idx_token_attributes_value ON token_attributes (trait_value);
