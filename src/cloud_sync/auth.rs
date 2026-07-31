#[derive(Clone)]
pub struct CloudAuthenticator {
    secret: String,
}

impl CloudAuthenticator {
    pub fn new(secret: impl Into<String>) -> Self {
        Self { secret: secret.into() }
    }

    pub fn validate_token(&self, token: &str) -> bool {
        let clean_token = token.replace("Bearer ", "").trim().to_string();
        !clean_token.is_empty() && clean_token == self.secret
    }
}