# Experimental Concurrency Control CRUD B+Tree Implementation
    Build:             - 10.03.2023
    Version:           - 0.0.65 (Experimental)
### Platform
    - Architecture:  x86, ARM
    - OS:            Linux, Windows
    - Rustc:         >= 1.65.0 (2021 Edition)
---------------------------------------
# Locking Strategies Implemented
    - Mono (Single User)
    - Mutually-Exclusive Lock Coupling (Multi-User)
    - Readers-Writer Lock Coupling (Multi-User)
    - Optimistic Lock Coupling (Multi-User)

# Querying Support
    - (C) Create  - Insert a new key
    - (R) Read    - Read a single key or multiple keys
    - (U) Update  - Update an existing key
    - (D) Delete  - NOT SUPPORTED YET
---------------------------------------
# Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de