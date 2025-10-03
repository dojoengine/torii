CREATE INDEX IF NOT EXISTS idx_entity_model_model_entity ON entity_model (model_id, entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_model_covering ON entity_model (model_id, entity_id, historical_counter);

CREATE INDEX IF NOT EXISTS idx_entities_event_id_id ON entities (event_id DESC, id);
CREATE INDEX IF NOT EXISTS idx_entities_executed_at_id ON entities (executed_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_entities_updated_at_id ON entities (updated_at DESC, id);

CREATE INDEX IF NOT EXISTS idx_entities_historical_model_event ON entities_historical (model_id, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_entities_historical_id_model ON entities_historical (id, model_id);

CREATE INDEX IF NOT EXISTS idx_event_model_model_entity ON event_model (model_id, entity_id);
CREATE INDEX IF NOT EXISTS idx_event_model_covering ON event_model (model_id, entity_id, historical_counter);

CREATE INDEX IF NOT EXISTS idx_event_messages_event_id_id ON event_messages (event_id DESC, id);
CREATE INDEX IF NOT EXISTS idx_event_messages_executed_at_id ON event_messages (executed_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_event_messages_updated_at_id ON event_messages (updated_at DESC, id);

CREATE INDEX IF NOT EXISTS idx_event_messages_historical_model_event ON event_messages_historical (model_id, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_event_messages_historical_id_model ON event_messages_historical (id, model_id);
