pub mod arithmetic;
pub mod comparsion;
pub mod reduce;

use crate::{
    op::{BinaryOpKind, OpRef},
    programbuilder::SharedProgram,
};

pub struct Element {
    parent_program: SharedProgram,
    pub(crate) opref: OpRef,
}

impl Element {
    pub fn new(parent_program: SharedProgram, opref: OpRef) -> Element {
        Element {
            parent_program,
            opref,
        }
    }
}
