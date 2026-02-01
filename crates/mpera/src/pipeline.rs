use crate::{Result, artifact::Artifact, codegen::Codegen, flag::debug_level, program::Program};

pub struct Pipeline {
    program: Program,
}

impl Pipeline {
    pub fn new(program: Program) -> Pipeline {
        Pipeline { program }
    }

    pub fn run(self) -> Result<Artifact> {
        // println!("{}", self.program.oppool());
        let codegen = Codegen::new(self.program);
        let codegen = codegen.codegen_flat_linear()?;

        if debug_level() == Some(2) {
            println!("------ CODEGEN --------");
            println!(
                "{}",
                &codegen
                    .split("\n")
                    .enumerate()
                    .map(|(ix, x)| format!("{} | {x}", ix + 1))
                    .collect::<Vec<String>>()
                    .join("\n")
            );
            println!("-----------------------");
        }

        let artifact = Artifact::new(&codegen)?;
        Ok(artifact)
    }
}

// API quickhand
pub fn compile_program(program: Program) -> Result<Artifact> {
    let pipeline = Pipeline::new(program);
    let artifact = pipeline.run();
    artifact
}
