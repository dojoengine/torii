pub struct Update<T> {
    pub inner: T,
    pub optimistic: bool,
}

pub type EntityUpdate = Update<torii_proto::schema::Entity<false>>;
pub type EventMessageUpdate = Update<torii_proto::schema::Entity<true>>;
pub type ContractUpdate = Update<torii_proto::ContractCursor>;
pub type TransactionUpdate = Update<torii_proto::Transaction>;
pub type ModelRegistered = Update<torii_proto::Model>;
pub type TokenRegistered = Update<torii_proto::Token>;
pub type TokenBalanceUpdated = Update<torii_proto::TokenBalance>;
pub type EventEmitted = Update<torii_proto::Event>;