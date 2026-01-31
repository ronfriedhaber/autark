#[derive(Debug, Clone, Copy)]
pub struct OpRef(pub(crate) usize);

impl OpRef {
    pub(crate) fn new(index: usize) -> OpRef {
        OpRef(index)
    }
}
