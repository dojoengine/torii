use dojo_types::schema::Ty;
use starknet_crypto::Felt;

use torii_proto::{
    Clause, ComparisonOperator, KeysClause, LogicalOperator, MemberValue, PatternMatching
};

pub mod entity;
pub mod error;
pub mod event;
pub mod event_message;
pub mod indexer;
pub mod token;
pub mod token_balance;

pub(crate) fn match_entity(
    id: Felt,
    keys: &[Felt],
    updated_model: &Option<Ty>,
    clause: &Clause,
) -> bool {
    match clause {
        Clause::HashedKeys(hashed_keys) => hashed_keys.is_empty() || hashed_keys.contains(&id),
        Clause::Keys(clause) => {
            // Check model matching if specified in the clause
            if !clause.models.is_empty() {
                if let Some(updated_model) = &updated_model {
                    let name = updated_model.name();
                    // Split name into namespace and model parts
                    let (namespace, name) = name.split_once('-').unwrap_or(("", &name));

                    // Check if any model clause matches
                    if !clause.models.iter().any(|clause_model| {
                        if clause_model.is_empty() {
                            return true;
                        }

                        let (clause_namespace, clause_model) =
                            clause_model.split_once('-').unwrap_or((clause_model, ""));

                        // Match namespace and model name according to rules:
                        // - Empty or * namespace matches any namespace
                        // - Empty or * model matches any model in the specified namespace
                        (clause_namespace.is_empty()
                            || clause_namespace == "*"
                            || clause_namespace == namespace)
                            && (clause_model.is_empty()
                                || clause_model == "*"
                                || clause_model == name)
                    }) {
                        return false;
                    }
                } else {
                    // No model available but models specified in clause
                    return false;
                }
            }

            // Check key pattern matching
            if clause.pattern_matching == PatternMatching::FixedLen
                && keys.len() != clause.keys.len()
            {
                return false;
            }

            // Check if all keys match the pattern
            keys.iter().enumerate().all(|(idx, key)| {
                match clause.keys.get(idx) {
                    // Specific key requirement at this position
                    Some(Some(sub_key)) => key == sub_key,
                    // No specific requirement (None or position beyond clause.keys)
                    _ => true,
                }
            })
        }
        Clause::Member(member_clause) => {
            let updated_model = match updated_model {
                Some(model) => model,
                None => return false, // No model to match against
            };

            // Check if model name matches
            if updated_model.name() != member_clause.model {
                return false;
            }

            // Split the member path
            let parts = member_clause.member.split('.').collect::<Vec<&str>>();

            // Traverse the model structure to find the target member
            let mut current_ty = updated_model.clone();
            for (idx, part) in parts.iter().enumerate() {
                match &current_ty {
                    Ty::Struct(struct_ty) => {
                        // Find the member with matching name
                        if let Some(member) = struct_ty.children.iter().find(|c| c.name == *part) {
                            current_ty = member.ty.clone();
                        } else {
                            return false; // Member not found
                        }
                    }
                    Ty::Tuple(tuple_ty) => {
                        // Access tuple element by index
                        if let Ok(index) = part.parse::<usize>() {
                            if let Some(ty) = tuple_ty.get(index) {
                                current_ty = ty.clone();
                            } else {
                                return false; // Index out of bounds
                            }
                        } else {
                            return false; // Invalid index
                        }
                    }
                    Ty::Enum(enum_ty) => {
                        // Special handling for enums
                        let is_last_part = idx == parts.len() - 1;

                        if is_last_part {
                            // If it's the last part and we're checking the enum itself,
                            // compare the selected option's name
                            let option = match enum_ty.option() {
                                Ok(opt) => opt,
                                Err(_) => return false, // No enum option selected
                            };

                            // Compare enum option name with clause value
                            return match (member_clause.operator.clone(), &member_clause.value) {
                                (ComparisonOperator::Eq, MemberValue::String(value)) => {
                                    option.name == *value
                                }
                                (ComparisonOperator::Neq, MemberValue::String(value)) => {
                                    option.name != *value
                                }
                                (ComparisonOperator::In, MemberValue::List(values)) => {
                                    values.iter().any(|v| match v {
                                        MemberValue::String(s) => option.name == *s,
                                        _ => false,
                                    })
                                }
                                (ComparisonOperator::NotIn, MemberValue::List(values)) => {
                                    !values.iter().any(|v| match v {
                                        MemberValue::String(s) => option.name == *s,
                                        _ => false,
                                    })
                                }
                                _ => false, // Other operators don't make sense for enum names
                            };
                        } else {
                            // If it's not the last part, find the enum option and continue navigating
                            if let Some(option_idx) =
                                enum_ty.options.iter().position(|o| o.name == *part)
                            {
                                if Some(option_idx as u8) == enum_ty.option {
                                    // If this option is selected, continue with its type
                                    current_ty = enum_ty.options[option_idx].ty.clone();
                                } else {
                                    return false; // Option not selected
                                }
                            } else {
                                return false; // Option not found
                            }
                        }
                    }
                    Ty::ByteArray(string) => {
                        // Handle ByteArray comparisons
                        if idx == parts.len() - 1 {
                            return match (member_clause.operator.clone(), &member_clause.value) {
                                (ComparisonOperator::Eq, MemberValue::String(value)) => {
                                    string == value
                                }
                                (ComparisonOperator::Neq, MemberValue::String(value)) => {
                                    string != value
                                }
                                (ComparisonOperator::Gt, MemberValue::String(value)) => {
                                    string > value
                                }
                                (ComparisonOperator::Gte, MemberValue::String(value)) => {
                                    string >= value
                                }
                                (ComparisonOperator::Lt, MemberValue::String(value)) => {
                                    string < value
                                }
                                (ComparisonOperator::Lte, MemberValue::String(value)) => {
                                    string <= value
                                }
                                (ComparisonOperator::In, MemberValue::List(values)) => {
                                    values.iter().any(|v| match v {
                                        MemberValue::String(s) => string == s,
                                        _ => false,
                                    })
                                }
                                (ComparisonOperator::NotIn, MemberValue::List(values)) => {
                                    !values.iter().any(|v| match v {
                                        MemberValue::String(s) => string == s,
                                        _ => false,
                                    })
                                }
                                _ => false,
                            };
                        } else {
                            return false; // Cannot navigate further into a ByteArray
                        }
                    }
                    Ty::Primitive(primitive) => {
                        // Handle primitive value comparisons
                        if idx == parts.len() - 1 {
                            return match (member_clause.operator.clone(), &member_clause.value) {
                                (ComparisonOperator::Eq, MemberValue::Primitive(value)) => {
                                    primitive == value
                                }
                                (ComparisonOperator::Neq, MemberValue::Primitive(value)) => {
                                    primitive != value
                                }
                                (ComparisonOperator::Gt, MemberValue::Primitive(value)) => {
                                    primitive > value
                                }
                                (ComparisonOperator::Gte, MemberValue::Primitive(value)) => {
                                    primitive >= value
                                }
                                (ComparisonOperator::Lt, MemberValue::Primitive(value)) => {
                                    primitive < value
                                }
                                (ComparisonOperator::Lte, MemberValue::Primitive(value)) => {
                                    primitive <= value
                                }
                                (ComparisonOperator::In, MemberValue::List(values)) => {
                                    values.iter().any(|v| match v {
                                        MemberValue::Primitive(p) => primitive == p,
                                        _ => false,
                                    })
                                }
                                (ComparisonOperator::NotIn, MemberValue::List(values)) => {
                                    !values.iter().any(|v| match v {
                                        MemberValue::Primitive(p) => primitive == p,
                                        _ => false,
                                    })
                                }
                                _ => false,
                            };
                        } else {
                            return false; // Cannot navigate further into a primitive
                        }
                    }
                    Ty::Array(_) => {
                        // Array navigation would be more complex and is not handled
                        // in the original code. Could be added if needed.
                        return false;
                    }
                }
            }

            // If we reach here, we've navigated the full path but haven't hit a
            // comparison case, which shouldn't happen with proper member paths
            false
        }
        Clause::Composite(composite_clause) => match composite_clause.operator {
            LogicalOperator::And => composite_clause
                .clauses
                .iter()
                .all(|c| match_entity(id, keys, updated_model, c)),
            LogicalOperator::Or => composite_clause
                .clauses
                .iter()
                .any(|c| match_entity(id, keys, updated_model, c)),
        },
    }
}

pub(crate) fn match_keys(keys: &[Felt], clauses: &[KeysClause]) -> bool {
    // Check if the subscriber is interested in this entity
    // If we have a clause of hashed keys, then check that the id of the entity
    // is in the list of hashed keys.

    // If we have a clause of keys, then check that the key pattern of the entity
    // matches the key pattern of the subscriber.
    if !clauses.is_empty()
        && !clauses.iter().any(|clause| {
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
        })
    {
        return false;
    }

    true
}
