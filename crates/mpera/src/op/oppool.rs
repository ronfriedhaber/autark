use std::fmt::{self};

use crate::op::{Op, OpRef};

#[derive(Debug, Clone)]
pub struct OpPool(Vec<Op>);

impl OpPool {
    pub(crate) fn new(initial_capacity: usize) -> OpPool {
        OpPool(Vec::with_capacity(initial_capacity))
    }

    pub(crate) fn get(&self, opref: OpRef) -> Option<&Op> {
        Some(self.0.get(opref.0)?)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn insert(&mut self, op: Op) -> OpRef {
        let ix = self.0.len();
        self.0.push(op);

        OpRef(ix)
    }
}

impl<'a> IntoIterator for &'a OpPool {
    type Item = &'a Op;
    type IntoIter = std::slice::Iter<'a, Op>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl fmt::Display for OpPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().enumerate().for_each(|(ix, x)| {
            write!(f, "OP{} ", ix);
            // x.fmt(f);
            write!(f, "\n");
        });
        Ok(())
    }
}
