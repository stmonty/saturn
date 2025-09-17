pub mod crc32c {
    // Reflected polynomial for CRC32C (Castagnoli).
    // Using the standard reflected algorithm:
    // init = 0xFFFF_FFFF, process LSB-first, xorout = 0xFFFF_FFFF.
    const POLY_REFLECTED: u32 = 0x82F6_3B78;

    #[inline]
    fn update_byte(mut crc: u32, b: u8) -> u32 {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg() & POLY_REFLECTED;
            crc = (crc >> 1) ^ mask;
        }
        crc
    }

    #[inline]
    pub fn value(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc = update_byte(crc, b);
        }
        crc ^ 0xFFFF_FFFF
    }

    // Extend a prior CRC with more bytes (i.e., crc(data0 || data1)).
    #[inline]
    pub fn extend(initial_crc: u32, data: &[u8]) -> u32 {
        let mut crc = initial_crc ^ 0xFFFF_FFFF;
        for &b in data {
            crc = update_byte(crc, b);
        }
        crc ^ 0xFFFF_FFFF
    }

    // Same masking LevelDB uses: rotate-right by 15, add a constant.
    #[inline]
    pub fn mask(crc: u32) -> u32 {
        ((crc >> 15) | (crc << 17)).wrapping_add(0xA282_EAD8)
    }

    #[allow(dead_code)]
    #[inline]
    pub fn unmask(masked: u32) -> u32 {
        let rot = masked.wrapping_sub(0xA282_EAD8);
        (rot << 15) | (rot >> 17)
    }

    #[inline]
    pub fn get_fixed32_le(src4: &[u8]) -> u32 {
        // src4 must be at least 4 bytes long; callers pass header[0..4].
        // Little-endian decode: b0 + b1<<8 + b2<<16 + b3<<24
        (src4[0] as u32)
            | ((src4[1] as u32) << 8)
            | ((src4[2] as u32) << 16)
            | ((src4[3] as u32) << 24)
    }

    #[inline]
    pub fn put_fixed32_le(dst4: &mut [u8], v: u32) {
        dst4[0] = (v & 0xFF) as u8;
        dst4[1] = ((v >> 8) & 0xFF) as u8;
        dst4[2] = ((v >> 16) & 0xFF) as u8;
        dst4[3] = ((v >> 24) & 0xFF) as u8;
    }
}

#[test]
fn fixed32_roundtrip() {
    let n = 0xA1B2_C3D4u32;
    let mut b = [0u8; 4];
    crc32c::put_fixed32_le(&mut b, n);
    assert_eq!(crc32c::get_fixed32_le(&b), n);
    assert_eq!(u32::from_le_bytes(b), n);
}
