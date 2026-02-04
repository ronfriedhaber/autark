use std::sync::{Arc, RwLock};

use crate::{
    Result,
    error::Error,
    op::{BinaryOpKind, JoinKind, Op, OpPool, OpRef, ReduceOpKind},
    programmetadata::ProgramMetadata,
};
use arrow::datatypes::Schema;

#[derive(Clone)]
pub struct Program {
    pub(crate) op_pool: Arc<RwLock<OpPool>>,
    // metdata is too behind an Arc<RwLock<_>> as, like with op_pool, it is expected of a program to include a single, consolidated, metadata.
    pub(crate) metadata: Arc<RwLock<ProgramMetadata>>,
    root: Option<OpRef>,
}

impl Program {
    pub fn new() -> Program {
        let op_pool = Arc::new(RwLock::new(OpPool::new(1024)));
        Program {
            op_pool,
            metadata: Arc::new(RwLock::new(ProgramMetadata::default())),
            root: None,
        }
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.op_pool.read().map_err(|_| Error::PoisonedLock)?.len())
    }

    fn with_generic(&self, op: Op) -> Result<Program> {
        let opref = self
            .op_pool
            .write()
            .map_err(|_| Error::PoisonedLock)?
            .insert(op);

        Ok(Self {
            op_pool: self.op_pool.clone(),
            metadata: self.metadata.clone(),

            root: Some(opref),
        })
    }

    pub(crate) fn get_op(&self, opref: OpRef) -> Option<Op> {
        match self.op_pool.read().unwrap().get(opref) {
            Some(x) => Some(x.clone()),
            None => None,
        } // TODO: Not clone
    }

    fn root(&self) -> Result<OpRef> {
        match self.root {
            Some(x) => Ok(x),
            None => Err(Error::ProvidedEmptyProgram),
        }
    }

    pub fn dataframe(&self, index: Option<usize>) -> Result<Program> {
        self.with_generic(Op::DataFrame { index })
    }

    pub fn col(&self, name: &str) -> Result<Program> {
        self.with_generic(Op::Column {
            dataframe: self.root()?,
            column: name.to_string(),
        })
    }

    pub fn concat(&self, who: &[Program]) -> Result<Program> {
        let mut op_refs = Vec::with_capacity(who.len() + 1);
        op_refs.push(self.root()?);
        for program in who {
            op_refs.push(program.root()?);
        }

        self.with_generic(Op::Concat { who: op_refs })
    }

    pub fn slice(&self, start: isize, end: isize) -> Result<Program> {
        self.with_generic(Op::Slice {
            on: self.root()?,
            start,
            end,
        })
    }

    pub fn rolling(&self, n: usize) -> Result<Program> {
        self.with_generic(Op::Rolling {
            on: self.root()?,
            n,
        })
    }

    pub fn order_by(&self, by: Program, ascending: bool) -> Result<Program> {
        self.with_generic(Op::OrderBy {
            what: self.root()?,
            by: by.root()?,
            ascending,
        })
    }

    pub fn alias(&self, name: &str, schema: Option<Schema>) -> Result<Program> {
        if let Some(schema) = schema {
            self.metadata
                .write()
                .map_err(|_| Error::PoisonedLock)?
                .schema_map
                .insert(name.to_string(), schema);
        }

        self.with_generic(Op::Output {
            name: name.to_string(),
            value: self.root()?,
        })
    }

    pub fn binaryop(&self, rhs: Program, kind: BinaryOpKind) -> Result<Program> {
        self.with_generic(Op::BinaryOp {
            kind,
            lhs: self.root()?,
            rhs: rhs.root()?,
        })
    }

    pub fn filter(&self, mask: Program) -> Result<Program> {
        self.with_generic(Op::Filter {
            on: self.root()?,
            mask: mask.root()?,
        })
    }

    pub fn const_f64(&self, value: f64) -> Result<Program> {
        self.with_generic(Op::ConstantF64 { value })
    }

    pub fn reduce(&self, kind: ReduceOpKind) -> Result<Program> {
        self.with_generic(Op::Reduce {
            kind,
            on: self.root()?,
        })
    }

    pub fn group_by(&self, keys: Program, kind: ReduceOpKind) -> Result<Program> {
        self.with_generic(Op::GroupBy {
            keys: keys.root()?,
            values: self.root()?,
            kind,
        })
    }

    pub fn join(
        &self,
        right: Program,
        left_on: Program,
        right_on: Program,
        kind: JoinKind,
    ) -> Result<Program> {
        self.with_generic(Op::Join {
            left: self.root()?,
            right: right.root()?,
            left_on: left_on.root()?,
            right_on: right_on.root()?,
            kind,
        })
    }

    // pub fn const_f64(&mut self, value: f64) -> OpRef {
    //     self.op_pool.insert(Op::ConstantF64 { value })
    // }
}
