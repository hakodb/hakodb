use std::collections::BTreeMap;

pub fn prefix_scan(
    tree: &BTreeMap<Vec<u8>, String>,
    prefix: &[u8],
) -> Vec<String> {

    let mut result = Vec::new();

    for (k,v) in tree.range(prefix.to_vec()..) {

        if !k.starts_with(prefix) {
            break;
        }

        result.push(v.clone());
    }

    result
}
