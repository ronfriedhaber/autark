use std::sync::{Arc, RwLock};

use crate::{
    // element::Element,
    op::{BinaryOpKind, Op, OpPool, OpRef, ReduceOpKind},
};

#[derive(Debug, Clone)]
pub struct Program {
    pub(crate) op_pool: Arc<RwLock<OpPool>>,
    root: Option<OpRef>,
}

impl Program {
    pub fn new() -> Program {
        let op_pool = Arc::new(RwLock::new(OpPool::new(1024)));
        Program {
            op_pool,
            root: None,
        }
    }

    pub fn dataframe(&self, index: Option<usize>) -> Program {
        let opref = self
            .op_pool
            .write()
            .unwrap()
            .insert(Op::DataFrame { index });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn col(&self, name: &str) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Column {
            dataframe: self
                .root
                .expect("It seems as if you forgot to select a DataFrame to fetch a Column from."),
            column: name.to_string(),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn concat(&self, who: &[Program]) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Concat {
            who: [
                // terrible alloc
                vec![self.root.unwrap()],
                who.iter().map(|x| x.root.unwrap()).collect::<Vec<OpRef>>(),
            ]
            .concat(),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn slice(&self, start: isize, end: isize) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Slice {
            on: self.root.unwrap(),
            start,
            end,
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn rolling(&self, n: usize) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Rolling {
            on: self.root.unwrap(),
            n,
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn alias(&self, name: &str) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Output {
            name: name.to_string(),
            value: self
                .root
                .expect("Can't designate empty Program as part of output."),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn binaryop(&self, rhs: Program, kind: BinaryOpKind) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::BinaryOp {
            kind,
            lhs: self.root.unwrap(),
            rhs: rhs.root.unwrap(),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn filter(&self, mask: Program) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Filter {
            on: self.root.unwrap(),
            mask: mask.root.unwrap(),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn const_f64(&self, value: f64) -> Program {
        let opref = self
            .op_pool
            .write()
            .unwrap()
            .insert(Op::ConstantF64 { value });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    pub fn reduce(&self, kind: ReduceOpKind) -> Program {
        let opref = self.op_pool.write().unwrap().insert(Op::Reduce {
            kind,
            on: self.root.unwrap(),
        });

        Self {
            op_pool: self.op_pool.clone(),
            root: Some(opref),
        }
    }

    // pub fn const_f64(&mut self, value: f64) -> OpRef {
    //     self.op_pool.insert(Op::ConstantF64 { value })
    // }
}
