#[derive(Debug, Clone)]
pub struct Update<T> {
    pub inner: T,
    pub optimistic: bool,
}

impl<T> Update<T> {
    pub fn new(inner: T, optimistic: bool) -> Self {
        Self {
            inner,
            optimistic,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> From<T> for Update<T> {
    fn from(value: T) -> Self {
        Self::new(value, false)
    }
}

pub type EntityUpdate = Update<torii_proto::schema::Entity<false>>;
pub type EventMessageUpdate = Update<torii_proto::schema::Entity<true>>;
pub type ContractUpdate = Update<torii_proto::ContractCursor>;
pub type ModelRegistered = Update<torii_proto::Model>;
pub type TokenRegistered = Update<torii_proto::Token>;
pub type TokenBalanceUpdated = Update<torii_proto::TokenBalance>;
pub type EventEmitted = Update<torii_proto::Event>;
pub type Transaction = Update<torii_proto::Transaction>;