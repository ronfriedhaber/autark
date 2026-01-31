use crate::{
    Result,
    artifact::Artifact,
    codegen::Codegen,
    flag::{DebugLevel, debug_level},
    program::Program,
};

pub struct Pipeline {
    program: Program,
}

impl Pipeline {
    pub fn new(program: Program) -> Pipeline {
        Pipeline { program }
    }

    pub fn run(self) -> Artifact {
        // println!("{}", self.program.oppool());
        let codegen = Codegen::new(self.program);
        let codegen = codegen.codegen_flat_linear();

        // if debug_level() == Some(DebugLevel::II) {
        println!("------ CODEGEN --------");
        println!("{}", &codegen);
        println!("-----------------------");
        // }

        let artifact = Artifact::new(&codegen);
        artifact
    }
}

// API quickhand
pub fn compile_program(program: Program) -> Result<Artifact> {
    let pipeline = Pipeline::new(program);
    let artifact = pipeline.run();
    Ok(artifact)
}
