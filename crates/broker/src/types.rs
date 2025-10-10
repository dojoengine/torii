#[derive(Debug, Clone)]
pub struct Update<T> {
    pub inner: T,
    pub optimistic: bool,
}

impl<T> Update<T> {
    pub fn new(inner: T, optimistic: bool) -> Self {
        Self { inner, optimistic }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn is_optimistic(&self) -> bool {
        self.optimistic
    }
}

impl<T> From<T> for Update<T> {
    fn from(value: T) -> Self {
        Self::new(value, false)
    }
}

pub trait InnerType {
    type Inner;

    fn into_inner(self) -> Self::Inner;
}

impl<T> InnerType for Update<T> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.inner
    }
}

pub type EntityUpdate = Update<torii_proto::schema::EntityWithMetadata<false>>;
pub type EventMessageUpdate = Update<torii_proto::schema::EntityWithMetadata<true>>;
pub type ContractUpdate = Update<torii_proto::Contract>;
pub type ModelUpdate = Update<torii_proto::Model>;
pub type TokenUpdate = Update<torii_proto::Token>;
pub type TokenBalanceUpdate = Update<torii_proto::TokenBalance>;
pub type TokenTransferUpdate = Update<torii_proto::TokenTransfer>;
pub type EventUpdate = Update<torii_proto::EventWithMetadata>;
pub type TransactionUpdate = Update<torii_proto::Transaction>;
pub type AggregationUpdate = Update<torii_proto::AggregationEntry>;
pub type ActivityUpdate = Update<torii_proto::Activity>;
pub type AchievementProgressionUpdate = Update<torii_proto::AchievementProgression>;
