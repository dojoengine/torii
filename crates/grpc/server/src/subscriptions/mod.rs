use dojo_types::schema::Ty;
use starknet_crypto::{poseidon_hash_many, Felt};

use torii_proto::{Clause, ComparisonOperator, EntityKeysClause, MemberValue, PatternMatching};

pub mod entity;
pub mod error;
pub mod event;
pub mod event_message;
pub mod indexer;
pub mod token;
pub mod token_balance;

pub(crate) fn match_entity_keys(
    id: Felt,
    keys: &[Felt],
    updated_model: &Option<Ty>,
    clause: &Clause,
) -> bool {
    // Check if the subscriber is interested in this entity
    // If we have a clause of hashed keys, then check that the id of the entity
    // is in the list of hashed keys.

    // If we have a clause of keys, then check that the key pattern of the entity
    // matches the key pattern of the subscriber.
    match clause {
        Clause::HashedKeys(hashed_keys) => {
            hashed_keys.is_empty() || hashed_keys.contains(&id)
        }
        Clause::Keys(clause) => {
                // if we have a model clause, then we need to check that the entity
                // has an updated model and that the model name matches the clause
                if let Some(updated_model) = &updated_model {
                    let name = updated_model.name();
                    let (namespace, name) = name.split_once('-').unwrap();

                    if !clause.models.is_empty()
                        && !clause.models.iter().any(|clause_model| {
                            if clause_model.is_empty() {
                                return true;
                            }

                            let (clause_namespace, clause_model) =
                                clause_model.split_once('-').unwrap();
                            // if both namespace and model are empty, we should match all.
                            // if namespace is specified and model is empty or * we should
                            // match all models in the
                            // namespace if namespace
                            // and model are specified, we should match the
                            // specific model
                            (clause_namespace.is_empty()
                                || clause_namespace == namespace
                                || clause_namespace == "*")
                                && (clause_model.is_empty()
                                    || clause_model == name
                                    || clause_model == "*")
                        })
                    {
                        return false;
                    }
                }

                // if the key pattern doesnt match our subscribers key pattern, skip
                // ["", "0x0"] would match with keys ["0x...", "0x0", ...]
                if clause.pattern_matching == PatternMatching::FixedLen
                    && keys.len() != clause.keys.len()
                {
                    return false;
                }

                keys.iter().enumerate().all(|(idx, key)| {
                    // this is going to be None if our key pattern overflows the subscriber
                    // key pattern in this case we should skip
                    let sub_key = clause.keys.get(idx);

                    match sub_key {
                        // the key in the subscriber must match the key of the entity
                        // athis index
                        Some(Some(sub_key)) => key == sub_key,
                        // otherwise, if we have no key we should automatically match.
                        // or.. we overflowed the subscriber key pattern
                        // but we're in VariableLen pattern matching
                        // so we should match all next keys
                        _ => true,
                }
            })
        }
        Clause::Member(member_clause) => {
            if let Some(updated_model) = &updated_model {
                if updated_model.name() != member_clause.model {
                    return false;
                }

                let parts = member_clause.member.split('.').collect::<Vec<&str>>();
                let mut parent = updated_model.clone();
                for (idx, part) in parts.iter().enumerate() {
                    match parent {
                        Ty::Struct(struct_ty) => {
                            parent = struct_ty.children.iter().find(|c| c.name == part).map(|c| &c.ty);
                        }
                        Ty::Primitive(primitive) => {
                            return match (member_clause.operator, member_clause.value) {
                                (ComparisonOperator::Eq, MemberValue::Primitive(clause_primitive)) => primitive == clause_primitive,
                                (ComparisonOperator::Neq, MemberValue::Primitive(clause_primitive)) => primitive != clause_primitive,
                                (ComparisonOperator::Gt, MemberValue::Primitive(clause_primitive)) => primitive > clause_primitive,
                                (ComparisonOperator::Gte, MemberValue::Primitive(clause_primitive)) => primitive >= clause_primitive,
                                (ComparisonOperator::Lt, MemberValue::Primitive(clause_primitive)) => primitive < clause_primitive,
                                (ComparisonOperator::Lte, MemberValue::Primitive(clause_primitive)) => primitive <= clause_primitive,
                                _ => false,
                            }
                        }
                        _ => return false,
                    }
                }
            }

            false
        }
        Clause::Composite(_) => false,
    }
}

pub(crate) fn match_keys(keys: &[Felt], clauses: &[EntityKeysClause]) -> bool {
    // Check if the subscriber is interested in this entity
    // If we have a clause of hashed keys, then check that the id of the entity
    // is in the list of hashed keys.

    // If we have a clause of keys, then check that the key pattern of the entity
    // matches the key pattern of the subscriber.
    if !clauses.is_empty()
        && !clauses.iter().any(|clause| match clause {
            EntityKeysClause::HashedKeys(hashed_keys) => {
                hashed_keys.is_empty() || hashed_keys.contains(&poseidon_hash_many(keys))
            }
            EntityKeysClause::Keys(clause) => {
                // if the key pattern doesnt match our subscribers key pattern, skip
                // ["", "0x0"] would match with keys ["0x...", "0x0", ...]
                if clause.pattern_matching == PatternMatching::FixedLen
                    && keys.len() != clause.keys.len()
                {
                    return false;
                }

                keys.iter().enumerate().all(|(idx, key)| {
                    // this is going to be None if our key pattern overflows the subscriber
                    // key pattern in this case we should skip
                    let sub_key = clause.keys.get(idx);

                    match sub_key {
                        // the key in the subscriber must match the key of the entity
                        // athis index
                        Some(Some(sub_key)) => key == sub_key,
                        // otherwise, if we have no key we should automatically match.
                        // or.. we overflowed the subscriber key pattern
                        // but we're in VariableLen pattern matching
                        // so we should match all next keys
                        _ => true,
                    }
                })
            }
        })
    {
        return false;
    }

    true
}
