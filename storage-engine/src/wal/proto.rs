use crate::types::DataPoint;
#[derive(prost::Message)]
pub struct WalEntry {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(message, repeated, tag = "2")]
    pub points: Vec<WalDataPoint>,
}

#[derive(prost::Message)]
pub struct WalDataPoint {
    #[prost(string, tag = "1")]
    pub metric_name: String,
    #[prost(btree_map = "string, string", tag = "2")]
    pub tags: std::collections::BTreeMap<String, String>,
    #[prost(sint64, tag = "3")]
    pub timestamp_ns: i64,
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
