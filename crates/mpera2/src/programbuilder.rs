use std::sync::{Arc, RwLock};

use crate::{element::Element, program::Program};
pub(crate) type SharedProgram = Arc<RwLock<Program>>;

#[derive(Debug)]
pub struct ProgramBuilder {
    program: SharedProgram,
}

impl ProgramBuilder {
    pub fn new() -> ProgramBuilder {
        ProgramBuilder {
            program: Arc::new(RwLock::new(Program::new())),
        }
    }

    pub fn col(&self, name: &str) -> Element {
        let element = Element::new(
            self.program.clone(),
            self.program.write().unwrap().col(name),
        );
        element
    }

    pub fn const_f64(&mut self, value: f64) -> Element {
        let element = Element::new(
            self.program.clone(),
            self.program.write().unwrap().const_f64(value),
        );
        element
    }

    pub fn set_output(&self, name: &str, value: &Element) -> crate::op::OpRef {
        self.program.write().unwrap().set_output(name, value)
    }

    pub fn build(self) -> Program {
        self.program.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{program::Program, programbuilder::ProgramBuilder};

    #[test]
    fn test_0() {
        let mut prog = ProgramBuilder::new();
        let a = prog.col("a");
        let b = prog.col("b");
        // a + b;
        dbg!(prog);
    }
}
