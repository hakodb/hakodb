use crate::document::firelite_doc::Value;
use super::definition::CompositeIndexDefinition;

pub struct ScanRange {

    pub start: Vec<u8>,
    pub end: Vec<u8>,

}

pub struct RangeBuilder;

impl RangeBuilder {

    pub fn build_prefix(
        def: &CompositeIndexDefinition,
        values: &[Value],
    ) -> ScanRange {

        let mut start = Vec::new();
        let mut end = Vec::new();

        start.extend(&def.collection_id.to_be_bytes());
        end.extend(&def.collection_id.to_be_bytes());

        for v in values {

            start.push(1);
            end.push(1);

            match v {

                Value::Int(i) => {

                    start.extend(i.to_be_bytes());
                    end.extend(i.to_be_bytes());
                }

                _ => {}
            }
        }

        end.push(255);

        ScanRange { start, end }

    }

}
