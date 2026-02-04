# Internal Overview Of The Contemporary (Pre-Alpha) Autark Engine

**Epistemic Status: DRAFT**
**Valid As Of: 2026-02-03**

The core Autark system may be thought of as composed of two components — MPERA ("Massively Parallel Enhanced Relational Algebra") and Autark Dataframe, which provides user-facing APIs and peripherals.

**MPERA** seeks to be a frontend-agnostic OLAP processing framework — third **virtual machine**, third **compiler**, third **runtime**.

**Autark DataFrame**, of whom I expect to be split into succeeding subcrates in the near-future, currently encapsulates 5 abstract modules — `dataframe`, Which can be thought of as an intermidiate representation and entity between Apache Arrow and MPERA Compatible Formats. `reader` which implement's Autark's highly-modular reading primitives, and `sink`, which encapsulates logic and abstraction for "sinking" computational results to a non-volatile space. Both `reader` and `sink` exist as part of I/O.
