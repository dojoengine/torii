-- Add receipt_json column to store full transaction receipt data
ALTER TABLE transaction_receipts ADD COLUMN receipt_json TEXT;

