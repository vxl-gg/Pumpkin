//! Registry key normalization shared by SDK builders.

pub fn qualify(key: &str) -> String {
    if key.contains(':') {
        key.to_string()
    } else {
        format!("minecraft:{key}")
    }
}
