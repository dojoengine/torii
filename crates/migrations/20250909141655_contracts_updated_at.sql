-- Add updated_at column to contracts table (without default first)
ALTER TABLE contracts ADD COLUMN updated_at TIMESTAMP;

-- Initialize existing records with current timestamp
UPDATE contracts SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;