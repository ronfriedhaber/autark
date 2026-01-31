
pub struct Executable {
    source: String,
    // object: Option<Py<PyAny>>,
}

impl Executable {
    pub fn new(source: String) -> Executable {
        Executable {
            source,
            //     object: None,
        }
    }

    pub fn run(&mut self) {
        // Executable { source, object: () }
        ()
    }
}
