# Contemporary MPERA Architecture Overview | 24 Jan 2026

The core idea is that any OLAP workload can be defined as a single, pure, transformation function. MPERA (Massively-Parallel-Enhanced-Relation Algebra) is a quasi-compiler enabling the expression of such transforms, their transpilation to an IR, and consequent codegen.

There presently exists 3 main modules, each including multiple sub-modules.

## Program
The program layer, which greatly resembles a traditional frontend, enables and incorporates the user-defined program, which exists as a quasi Direct Acyiclic Graph (DAG). 

## Pipeline
The pipeline module is reponsible for valorizing a Program to a Compiled Artifact. Currently, it solely executes codegen. In the future, it may include various "midend" jobs.

## Runtime
Currently incredibly straight-forward, accepts a Compiled Artifact, and handles execution of the function with  *proper and prefferably valid* input.
