use std::collections::BTreeMap;

pub fn prefix_scan(tree: &BTreeMap<Vec<u8>, String>, prefix: &[u8]) -> Vec<String> {
    tree.range(prefix.to_vec()..)
        .take_while(|(k, _)| k.starts_with(prefix))
        .map(|(_, v)| v.clone())
        .collect()
}
