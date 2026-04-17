pub fn encode_varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn decode_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut res = 0u64;
    let mut shift = 0;
    while shift < 64 {
        let b = *bytes.get(*pos)? as u64;
        *pos += 1;
        res |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            return Some(res);
        }
        shift += 7;
    }
    None
}

pub fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

pub fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}