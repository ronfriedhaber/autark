use crate::{
    op::{BinaryOpKind, OpRef, ReduceOpKind},
    program::Program,
};

pub struct Codegen {
    program: Program,
}

fn codegen_var_stmt_vanilla(name: usize, body: &str) -> String {
    format!("\tx{name} = {body};\n")
}

impl Codegen {
    pub fn new(program: Program) -> Codegen {
        Codegen { program }
    }

    fn binaryop(ix: usize, kind: &BinaryOpKind, lhs: &OpRef, rhs: &OpRef) -> String {
        codegen_var_stmt_vanilla(
            ix,
            &format!(
                "x{} {} x{}",
                lhs.0,
                match kind {
                    BinaryOpKind::Add => "+",
                    BinaryOpKind::Sub => "-",
                    BinaryOpKind::Mul => "*",
                    BinaryOpKind::Div => "/",

                    BinaryOpKind::LesserThan => "<",
                    BinaryOpKind::GreaterThan => ">",
                    BinaryOpKind::LesserEquals => "<=",
                    BinaryOpKind::GreaterEquals => ">=",
                },
                rhs.0
            ),
        )
    }

    fn reduce(ix: usize, kind: &ReduceOpKind, on: &OpRef) -> String {
        codegen_var_stmt_vanilla(
            ix,
            &format!(
                "x{}.{}()",
                on.0,
                match kind {
                    ReduceOpKind::Sum => "sum",
                    ReduceOpKind::Product => "product",
                    ReduceOpKind::Mean => "mean",
                }
            ),
        )
    }

    pub fn codegen_flat_linear(&self) -> String {
        use crate::op::Op::*;
        let o: String = self
            .program
            .op_pool
            .read()
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(ix, x)| match x {
                BinaryOp { kind, lhs, rhs } => Self::binaryop(ix, &kind, &lhs, &rhs),
                Reduce { kind, on } => Self::reduce(ix, &kind, &on),
                Column { dataframe, column } => codegen_var_stmt_vanilla(
                    ix,
                    format!("dfs[0][name2index[0]['{column}']]").as_str(),
                ),
                Output { name, value } => {
                    codegen_var_stmt_vanilla(ix, &format!("output['{}'] = x{}", name, value.0))
                }
                ConstantF64 { value } => codegen_var_stmt_vanilla(ix, &format!("float({})", value)),
                _ => todo!(),
            })
            .collect();

        // let output_body: String = self
        //     .program
        //     .oppool()
        //     .into_iter()
        //     .enumerate()
        //     .map(|(ix, _)| format!("x{ix}, "))
        //     .collect();

        format!(
            "from tinygrad import Tensor, TinyJit\nfrom typing import *\n\n\ndef transform(dfs: List[Tensor], name2index: Dict[str, int]):\n\toutput = {{}};\n{}\n\toutput = {{k: output[k].realize().tolist() for k in output}};\n\treturn output",
            o,
            // &output_body[0..output_body.len() - 2]
        )
    }
}
