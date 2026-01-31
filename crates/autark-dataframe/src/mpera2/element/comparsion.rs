use crate::{element::Element, op::BinaryOpKind};

macro_rules! impl_comparsion_op {
    ($fn_name:ident, $kind_name:ident) => {
        impl Element {
            pub fn $fn_name<T: Into<Element>>(&self, rhs: T) -> Element {
                let opref = self.parent_program.write().unwrap().binaryop(
                    self.opref,
                    rhs.into().opref,
                    BinaryOpKind::$kind_name,
                );

                Element {
                    parent_program: self.parent_program.clone(),
                    opref,
                }
            }
        }
    };
}

impl_comparsion_op!(lesser_than, LesserThan);
impl_comparsion_op!(greater_than, GreaterThan);
impl_comparsion_op!(lesser_equals, LesserEquals);
impl_comparsion_op!(greater_equals, GreaterEquals);
