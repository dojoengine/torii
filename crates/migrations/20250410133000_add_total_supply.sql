-- Add total_supply column to tokens table for ERC20 tokens
ALTER TABLE tokens ADD COLUMN total_supply TEXT;

-- Create an index on total_supply for efficient queries
CREATE INDEX idx_tokens_total_supply ON tokens (total_supply) WHERE total_supply IS NOT NULL;