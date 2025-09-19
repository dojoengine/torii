-- Add traits column to tokens table to store NFT trait types and their possible values
-- This column stores a JSON object where keys are trait names and values are arrays of possible values
ALTER TABLE tokens ADD COLUMN traits TEXT DEFAULT '{}';

-- Add an index on the traits column for better query performance
CREATE INDEX idx_tokens_traits ON tokens (traits);
