use crate::{
    Result,
    error::Error,
    op::{BinaryOpKind, OpRef, ReduceOpKind},
    program::Program,
};

pub struct Codegen {
    program: Program,
}

fn codegen_var_stmt_vanilla(name: usize, body: &str) -> String {
    format!("\tx{name} = {body};print(f\"{{'{name}'}} = {{x{name}}}\")\n")
}

impl Codegen {
    pub fn new(program: Program) -> Codegen {
        Codegen { program }
    }

    fn binaryop(ix: usize, kind: &BinaryOpKind, lhs: &OpRef, rhs: &OpRef) -> String {
        codegen_var_stmt_vanilla(
            ix,
            &format!(
                // "(x{}[0] {} x{}[0]).unsqueeze(0)",
                "(x{} {} x{})",
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
                    BinaryOpKind::Equals => "==",
                    BinaryOpKind::NotEquals => "!=",
                },
                rhs.0
            ),
        )
    }

    fn reduce(ix: usize, kind: &ReduceOpKind, on: &OpRef) -> String {
        codegen_var_stmt_vanilla(
            ix,
            &format!(
                "x{}.{}(axis=-1)",
                on.0,
                match kind {
                    ReduceOpKind::Sum => "sum",
                    ReduceOpKind::Product => "product",
                    ReduceOpKind::Mean => "mean",
                    ReduceOpKind::Count => "len",
                    ReduceOpKind::Stdev => "std",
                }
            ),
        )
    }

    fn rolling(ix: usize, on: &OpRef, n: usize) -> String {
        codegen_var_stmt_vanilla(ix, &format!("_mpera_rolling(x{}, {})", on.0, n))
        // format!("", on.0, n)
    }

    pub fn codegen_flat_linear(&self) -> Result<String> {
        use crate::op::Op::*;
        let o: String = self
            .program
            .op_pool
            .read()
            .map_err(|_| Error::PoisonedLock)?
            .into_iter()
            .enumerate()
            .map(|(ix, x)| match x {
                BinaryOp { kind, lhs, rhs } => Self::binaryop(ix, &kind, &lhs, &rhs),
                Reduce { kind, on } => Self::reduce(ix, &kind, &on),
                Rolling { on, n } => Self::rolling(ix, &on, *n),
                OrderBy {
                    what,
                    by,
                    ascending,
                } => codegen_var_stmt_vanilla(
                    ix,
                    &format!(
                        "_mpera_orderby(x{}, key=x{}, ascending={})",
                        what.0,
                        by.0,
                        if *ascending { "True" } else { "False" }
                    ),
                ),
                Column { dataframe, column } => codegen_var_stmt_vanilla(
                    ix,
                    format!("x{}[name2index[0]['{column}']].unsqueeze(0)", dataframe.0).as_str(),
                ),

                Concat { who } => codegen_var_stmt_vanilla(
                    ix,
                    format!(
                        "x{}.cat({} ,dim=0)",
                        who[0].0,
                        &who[1..]
                            .iter()
                            .map(|x| format!("x{}", x.0))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                    .as_str(),
                ),
                DataFrame { index } => {
                    codegen_var_stmt_vanilla(ix, &format!("dfs[{}]", index.unwrap_or(0)))
                }
                Output { name, value } => {
                    format!("\toutput.append(('{}',  x{}));\n", name, value.0)
                }
                ConstantF64 { value } => {
                    codegen_var_stmt_vanilla(ix, &format!("Tensor([{}])", value))
                }
                Slice { on, start, end } => {
                    codegen_var_stmt_vanilla(ix, &format!("x{}[:, {}:{}]", on.0, start, end))
                }
                Filter { on, mask } => {
                    // codegen_var_stmt_vanilla(ix, &format!(r#"(x{}.masked_select(x{})  if isinstance(x{}, Tensor) else ([x{}, [  i.masked_select(x{}) for i in x{}[1] ]]))"#, on.0, mask.0, on.0, on.0, mask.0, on.0))
                    codegen_var_stmt_vanilla(
                        ix,
                        &format!(r#"x{}.masked_select(x{})"#, on.0, mask.0),
                    )
                }
            })
            .collect();

        // let output_body: String = self
        //     .program
        //     .oppool()
        //     .into_iter()
        //     .enumerate()
        //     .map(|(ix, _)| format!("x{ix}, "))
        //     .collect();

        Ok(format!(
            "{}\n\n\n\ndef transform(dfs: List[Tensor], name2index: Dict[str, int]):\n\toutput = [];\n{}\n\t\n\treturn output",
            include_str!("../templates/codegen/prefix.py"),
            o // &output_body[0..output_body.len() - 2]
        ))
    }
}
