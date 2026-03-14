use crate::document::firelite_doc::Value;

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct CompositeField {

    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub struct CompositeIndexDefinition {

    pub collection_id: u32,

    pub fields: Vec<CompositeField>,
}

impl CompositeIndexDefinition {

    pub fn new(collection_id: u32) -> Self {

        Self {
            collection_id,
            fields: Vec::new(),
        }
    }

    pub fn add_field(
        mut self,
        field: &str,
        direction: SortDirection,
    ) -> Self {

        self.fields.push(
            CompositeField {
                field: field.to_string(),
                direction,
            }
        );

        self
    }
}
