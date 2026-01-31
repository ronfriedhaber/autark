use super::*;

macro_rules! impl_op_arithmetic {
    ($trait_name:ident, $fn_name:ident, $kind_name: ident) => {
        impl std::ops::$trait_name<Self> for &Element {
            type Output = Element;

            fn $fn_name(self, rhs: Self) -> Self::Output {
                Element::new(
                    self.parent_program.clone(),
                    self.parent_program.write().unwrap().binaryop(
                        self.opref,
                        rhs.opref,
                        BinaryOpKind::$kind_name,
                    ),
                )
            }
        }
    };
}

impl_op_arithmetic!(Add, add, Add);
impl_op_arithmetic!(Sub, sub, Sub);
impl_op_arithmetic!(Mul, mul, Mul);
impl_op_arithmetic!(Div, div, Div);
