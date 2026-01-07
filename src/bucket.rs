pub struct BucketTime {
    timestamp: i64,
}

impl BucketTime {
    pub fn current() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let bucket_start = (now / 900) * 900; // Round down to nearest 900 seconds (15 min)
        Self {
            timestamp: bucket_start,
        }
    }

    pub fn from_timestamp(ts: i64) -> Self {
        let bucket_start = (ts / 900) * 900;
        Self {
            timestamp: bucket_start,
        }
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn slug(&self) -> String {
        format!("btc-updown-15m-{}", self.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_calculation() {
        let bucket = BucketTime::from_timestamp(1700000000);
        assert_eq!(bucket.timestamp() % 900, 0);
        assert!(bucket.slug().starts_with("btc-updown-15m-"));
    }
}
