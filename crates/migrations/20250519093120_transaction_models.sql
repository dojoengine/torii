-- Create a new table to store the transaction and model ids
CREATE TABLE IF NOT EXISTS transaction_models (
    transaction_hash TEXT NOT NULL,
    model_id TEXT NOT NULL,
    UNIQUE (transaction_hash, model_id),
    FOREIGN KEY (transaction_hash) REFERENCES transactions(id)
);

CREATE INDEX IF NOT EXISTS idx_transaction_models_transaction_hash ON transaction_models (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_transaction_models_model_id ON transaction_models (model_id);