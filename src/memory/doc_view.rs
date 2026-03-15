pub struct DocView<'a>{

    data:&'a [u8]

}

impl<'a> DocView<'a>{

    pub fn new(data:&'a [u8])->Self{

        Self{data}

    }

    pub fn get_u32(&self,offset:usize)->u32{

        let mut buf=[0u8;4];

        buf.copy_from_slice(
            &self.data[offset..offset+4]
        );

        u32::from_be_bytes(buf)

    }

    pub fn slice(
        &self,
        offset:usize,
        len:usize
    )->&[u8]{

        &self.data[offset..offset+len]

    }

}
