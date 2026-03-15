pub const PAGE_SIZE:usize = 4096;

pub struct Page {

    pub id:u64,

    pub data:Vec<u8>,

}

impl Page {

    pub fn new(id:u64)->Self{

        Self{
            id,
            data:vec![0;PAGE_SIZE]
        }

    }

    pub fn read(
        &self,
        offset:usize,
        len:usize
    )->&[u8]{

        &self.data[offset..offset+len]

    }

    pub fn write(
        &mut self,
        offset:usize,
        bytes:&[u8]
    ){

        let end = offset+bytes.len();

        self.data[offset..end].copy_from_slice(bytes);

    }

}
