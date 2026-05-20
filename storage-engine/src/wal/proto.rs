use crate::types::DataPoint;

/// A single WAL frame — one batch of data points written atomically.
#[derive(prost::Message)]
pub struct WalEntry {
    /// Monotonically increasing batch sequence number.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    /// Data points in this batch.
    #[prost(message, repeated, tag = "2")]
    pub points: Vec<WalDataPoint>,
}

/// Protobuf representation of a single data point for WAL serialization.
#[derive(prost::Message)]
pub struct WalDataPoint {
    /// Metric name.
    #[prost(string, tag = "1")]
    pub metric_name: String,
    /// Tag key-value pairs.
    #[prost(btree_map = "string, string", tag = "2")]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Unix timestamp in nanoseconds (sint64 for efficient encoding of negatives).
    #[prost(sint64, tag = "3")]
    pub timestamp_ns: i64,
    /// Observed value.
    #[prost(double, tag = "4")]
    pub value: f64,
}

impl From<&DataPoint> for WalDataPoint {
    fn from(pt: &DataPoint) -> Self {
        WalDataPoint {
            metric_name: pt.metric_name.clone(),
            tags: pt.tags.clone(),
            timestamp_ns: pt.timestamp_ns,
            value: pt.value,
        }
    }
}

impl From<WalDataPoint> for DataPoint {
    fn from(pt: WalDataPoint) -> Self {
        DataPoint {
            metric_name: pt.metric_name,
            tags: pt.tags,
            timestamp_ns: pt.timestamp_ns,
            value: pt.value,
        }
    }
}
