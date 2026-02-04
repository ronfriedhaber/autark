# Data Types

**Epistemic Status: DRAFT**
**Valid As Of: 2026-02-04**

* Floats (precision agnostic, for now.)
* Integers (precision agnostic, for now.)
* Booleans (precision agnostic, for now.)

## What About Strings?
Strings are currently represented as a `Variant` — `Variant` can be thought of as an alphabetically sorted enum with string representation.
Behind the scenes, it applies sorted dictionary encoding, which lives on traditional memory.
