pub fn put_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend(value.to_le_bytes());
}
