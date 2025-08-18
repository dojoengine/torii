-- Models now have a different serialized format, we add a new column `legacy_store` to
-- distinguish models that use the legacy format. This is important for backward compatibility.
ALTER TABLE models
ADD COLUMN legacy_store BOOLEAN NOT NULL;
