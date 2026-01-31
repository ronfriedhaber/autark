macro_rules! impl_op_reduce {
    ($fn_name:ident, $kind_name: ident) => {
        impl Element {
            pub fn $fn_name(&self) -> Element {
                Element::new(
                    self.parent_program.clone(),
                    self.parent_program
                        .write()
                        .unwrap()
                        .reduce(self.opref, ReduceOpKind::$kind_name),
                )
            }
        }
    };
}

pub use super::*;
pub use crate::op::*;

impl_op_reduce!(sum, Sum);
impl_op_reduce!(product, Product);
impl_op_reduce!(mean, Mean);
