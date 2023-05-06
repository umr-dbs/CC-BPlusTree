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