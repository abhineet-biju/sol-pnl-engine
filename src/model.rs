use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct RpcRequest<'a> {
    pub jsonrpc: &'static str,
    pub id: &'static str,
    pub method: &'static str,
    pub params: (&'a str, GetTransactionsConfig),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcSuccess<T> {
    pub result: GtfaPage<T>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcFailure {
    pub error: RpcErrorObject,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GtfaPage<T> {
    pub data: Vec<T>,
    #[serde(default)]
    pub pagination_token: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionDetails {
    Signatures,
    Full,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionsConfig {
    pub transaction_details: TransactionDetails,
    pub sort_order: SortOrder,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_supported_transaction_version: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commitment: Option<&'static str>,
}

impl GetTransactionsConfig {
    pub fn signatures(sort_order: SortOrder, limit: usize, filters: Option<Filters>) -> Self {
        Self {
            transaction_details: TransactionDetails::Signatures,
            sort_order,
            limit,
            pagination_token: None,
            filters,
            encoding: None,
            max_supported_transaction_version: None,
            commitment: Some("finalized"),
        }
    }

    pub fn full(sort_order: SortOrder, limit: usize, filters: Option<Filters>) -> Self {
        Self {
            transaction_details: TransactionDetails::Full,
            sort_order,
            limit,
            pagination_token: None,
            filters,
            encoding: Some("json"),
            max_supported_transaction_version: Some(0),
            commitment: Some("finalized"),
        }
    }

    pub fn with_pagination_token(mut self, pagination_token: Option<String>) -> Self {
        self.pagination_token = pagination_token;
        self
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<RangeFilterU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TransactionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_accounts: Option<TokenAccountsMode>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RangeFilterU64 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<u64>,
}

impl Filters {
    pub fn slot_range(start: u64, end: u64) -> Self {
        Self {
            slot: Some(RangeFilterU64 {
                gte: (start > 0).then_some(start),
                lte: (end < u64::MAX).then_some(end),
            }),
            status: Some(TransactionStatus::Any),
            token_accounts: Some(TokenAccountsMode::None),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionStatus {
    Succeeded,
    Failed,
    Any,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TokenAccountsMode {
    None,
    BalanceChanged,
    All,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::Filters;

    #[test]
    fn slot_range_omits_unbounded_edges() {
        let filters = Filters::slot_range(0, u64::MAX);
        let json = serde_json::to_value(&filters).expect("serialize filters");

        assert_eq!(
            json,
            serde_json::json!({
                "status": "any",
                "tokenAccounts": "none",
                "slot": {}
            })
        );
    }

    #[test]
    fn slot_range_keeps_bounded_edges() {
        let filters = Filters::slot_range(10, 20);
        let json = serde_json::to_value(&filters).expect("serialize filters");

        assert_eq!(
            json,
            serde_json::json!({
                "status": "any",
                "tokenAccounts": "none",
                "slot": {
                    "gte": 10,
                    "lte": 20
                }
            })
        );
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SignatureRecord {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub err: Option<Value>,
    pub memo: Option<String>,
    pub block_time: Option<i64>,
    pub confirmation_status: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullTransactionRecord {
    pub slot: u64,
    pub transaction_index: u32,
    pub block_time: Option<i64>,
    pub transaction: EncodedTransaction,
    pub meta: Option<TransactionMeta>,
}

impl FullTransactionRecord {
    pub fn primary_signature(&self) -> Option<&str> {
        self.transaction.signatures.first().map(String::as_str)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EncodedTransaction {
    pub signatures: Vec<String>,
    pub message: TransactionMessage,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransactionMessage {
    #[serde(rename = "accountKeys")]
    pub account_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMeta {
    #[serde(default)]
    pub err: Option<Value>,
    pub fee: u64,
    #[serde(rename = "preBalances")]
    pub pre_balances: Vec<u64>,
    #[serde(rename = "postBalances")]
    pub post_balances: Vec<u64>,
    #[serde(default)]
    pub loaded_addresses: Option<LoadedAddresses>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LoadedAddresses {
    #[serde(default)]
    pub writable: Vec<String>,
    #[serde(default)]
    pub readonly: Vec<String>,
}
