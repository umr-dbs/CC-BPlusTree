## Concurrency Control CRUD B+Tree
    Build:             - 09.04.2023
    Version:           - 0.0.78 (Beta)
    Enable OLC-HLE:    - cargo build --features hardware-lock-elision
    Architecture:      - x86, ARM (untested)
    OS:                - Linux, Windows
    Rustc:             - >= 1.65.0 (2021 Edition)
# Commands
    # Format: `command_name=<parameter0+parameter1+..>`
        *No Spaces or new lines*
    - crud_protocol | crud_protocols | crud | cruds | protocol | protocols
       --> Displays all crud protocol available.
    - info | system | sys
       --> Displays full system information.
    - make_splash | splash
       --> Displays build information.
    - yield_enabled | yield
       --> Displays true:
            if threads are yield (threads >> cpu threads)
       --> Displays false:
            otherwise and permits excessive threads utilization.
    - cpu_cores | cpu_threads | cpu
       --> Displays CPU core and thread count.
    -  generate | gen
       --> Stores random create data into file.
    - block_alignment | bsz_aln | alignment | aln | block | bsz
        --> Displays detailed block alignment settings.
    - hardware_lock_elision | hle
        --> Displays whether OLC HLE feature is enabled or not.
    - x86_64 | x86
        --> Displays CPU Architure.
    - simple_test | st
        --> Runs simple integrity tests.
    - create | c
        --> Runs Create benchmark.
        --> `c=[records_n0,records_n1,..]+
               [crudprotocol2,crudprotocol1,..]+
               [t0,t1,..]`
    - update_read | ur
         --> Runs Update+Read benchmark.
         --> `ur=records_n+
                 percent_updates+
                 [crudprotocol1,crudprotocol2,..]+
                 [t0,t1,..]
## Locking Techniques
#### Mono
        Name:   `MonoWriter`
        Object: `"MonoWriter"`

#### Lock-Coupling
        Name:   `LockCoupling`
        Object: `"LockCoupling"`
#### ORWC
        Name:   `ORWC(Attempts=<x>;Level=<Height|Const>)`
        Object: `{"ORWC":[{"Height":y_f32},x_usize]}`
        Object: `{"ORWC":[{"Const":y_u16},x_usize]}`
#### OLC
        Name:   `OLC`
        Object: `{"OLC":"Free"}`
#### Bounded OLC
    Name: 	`OLC-Bounded(Attempts=x;Level=<Height|Const>)`
    Object: `{"OLC":{"Bounded":{"attempts":x_usize,"level":{"Height":y_f32}}}}`
    Object: `{"OLC":{"Bounded":{"attempts":x_usize,"level":{"Const":y_u16}}}}`
#### Hybrid Locking
    Name: 	`HybridLock(Attempts=x;Level=<Height|Const>)`
    Object: `{"HybridLocking":[{"Height":y_f32},x_usize]}`
    Object: `{"HybridLocking":[{"Const":y_u16},x_usize]}`
#### Lightweight Hybrid Locking
    Name: 	`Lightweight-HybridLock(Attempts=x;Level=1*height)`
    Object: `{"OLC":{"Pinned":{"attempts":x_usize,"level":{"Height":y_f32}}}}`
    Object: `{"OLC":{"Pinned":{"attempts":x_usize,"level":{"Const":y_u16}}}}`
# CRUD
    - (C) Create  - Insert a new key
    - (R) Read    - Read a single key or multiple keys
    - (U) Update  - Update an existing key
    - (D) Delete  - NOT YET SUPPORTED
---------------------------------------
## Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de
