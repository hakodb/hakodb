#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeField {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeIndexDefinition {
    pub id: u32,
    pub collection: String,
    pub fields: Vec<CompositeField>,
}

impl CompositeIndexDefinition {
    pub fn new(collection: &str) -> Self {
        Self {
            id: 0,
            collection: collection.to_string(),
            fields: Vec::new(),
        }
    }

    pub fn with_fields(mut self, fields: Vec<(String, SortDirection)>) -> Self {
        self.fields = fields
            .into_iter()
            .map(|(field, direction)| CompositeField { field, direction })
            .collect();
        self
    }
}
