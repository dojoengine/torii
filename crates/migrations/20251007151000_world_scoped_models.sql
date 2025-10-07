-- Make models world-scoped by changing ID format from "model_selector" to "world_address:model_selector"
-- This prevents collisions when multiple worlds have models with the same selector

-- Add model_selector column for direct access (same pattern as entity_id)
ALTER TABLE models ADD COLUMN model_selector TEXT NOT NULL DEFAULT '';

-- Populate model_selector from id column (currently just the selector)
UPDATE models SET model_selector = id;

-- Now update id to be world-scoped: "world_address:model_selector"
-- First, create a backup of the old id for updating foreign key references
ALTER TABLE models ADD COLUMN old_id TEXT;
UPDATE models SET old_id = id;

-- Update id to be world-scoped format
UPDATE models 
SET id = world_address || ':' || model_selector
WHERE world_address != '' AND world_address IS NOT NULL;

-- Update entity_model foreign key references
UPDATE entity_model
SET model_id = (
    SELECT world_address || ':' || model_selector
    FROM models
    WHERE models.old_id = entity_model.model_id
);

-- Update event_model references (for event messages)
UPDATE event_model
SET model_id = (
    SELECT world_address || ':' || model_selector
    FROM models
    WHERE models.old_id = event_model.model_id
)
WHERE EXISTS (
    SELECT 1 FROM models WHERE models.old_id = event_model.model_id
);

-- Drop the backup column
ALTER TABLE models DROP COLUMN old_id;

-- Create index on model_selector for efficient lookups
CREATE INDEX idx_models_model_selector ON models (model_selector);

-- Composite index for queries filtering by both
CREATE INDEX idx_models_world_selector ON models (world_address, model_selector);

