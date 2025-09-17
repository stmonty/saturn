pub const BLOCK_SIZE: usize = 32 * 1024;
pub const HEADER_SIZE: usize = 4 + 2 + 1; // checksum(4) + length(2) + type(1)

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum RecordType {
    Zero = 0,  
    Full = 1,  
    First = 2,  
    Middle = 3, 
    Last = 4,
}
impl RecordType {
    fn as_u8(self) -> u8 { self as u8 }
}
pub const MAX_RECORD_TYPE: usize = RecordType::Last as usize;
