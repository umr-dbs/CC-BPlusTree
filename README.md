## Concurrency Control CRUD B+Tree
    Build:             - 26.03.2023
    Version:           - 0.0.76 (Beta)
    Enable OLC-HLE:    - cargo build --features hardware-lock-elision
### Platform
    - Architecture:  x86, ARM (untested)
    - OS:            Linux, Windows
    - Rustc:         >= 1.65.0 (2021 Edition)
## Locking Techniques
    - Mono
    - Lock-Coupling
    - ORWC
    - OLC
    - Bounded OLC 
    - Hybrid Locking
    - Lightweight Hybrid Locking

## CRUD
    - (C) Create  - Insert a new key
    - (R) Read    - Read a single key or multiple keys
    - (U) Update  - Update an existing key
    - (D) Delete  - NOT YET SUPPORTED
---------------------------------------
## Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de