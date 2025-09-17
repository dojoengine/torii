-- Indexes for tokens table
CREATE INDEX idx_tokens_contract_address_token_id
ON tokens (contract_address, token_id);

-- High priority indexes for token_balances table
-- Primary query pattern: filter by account_address (most common)
CREATE INDEX idx_token_balances_account_address
ON token_balances (account_address);

-- Account + contract queries
CREATE INDEX idx_token_balances_account_contract
ON token_balances (account_address, contract_address);

-- Most specific query pattern: account + contract + token_id
CREATE INDEX idx_token_balances_account_contract_token
ON token_balances (account_address, contract_address, token_id);
