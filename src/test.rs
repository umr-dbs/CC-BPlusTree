use std::collections::{HashSet, VecDeque};
use std::{env, fs, mem, path, thread};
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use rand::prelude::SliceRandom;
use rand::RngCore;
use sysinfo::{DiskExt, System, UserExt};
use crate::block::block::Block;
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::bplus_tree::BPlusTree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::locking::locking_strategy::{CRUDProtocol, hybrid_lock, lightweight_hybrid_lock, lightweight_hybrid_lock_read_attempts, lightweight_hybrid_lock_unlimited, LockingStrategy, olc, orwc, orwc_attempts};
use crate::page_model::BlockRef;
use crate::page_model::node::Node;
use crate::{make_splash, TREE, Tree, TreeDispatcher};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::{LockCoupling, MonoWriter};
use crate::utils::interval::Interval;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::ENABLE_YIELD;

pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;

pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize = BSZ / 8 / 2;
pub const NUM_RECORDS: usize = (BSZ - 2) / (8 + 8);


pub type Key = u64;
pub type Payload = f64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub type INDEX = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub(crate) const S_THREADS_CPU: [usize; 12] = [
    1,
    2,
    3,
    4,
    8,
    10,
    12,
    16,
    24,
    32,
    64,
    128,
    // 256,
    // 512,
    // 1024,
    // usize::MAX
];

pub(crate) const S_INSERTIONS: [Key; 1] = [
    // 10,
    // 100,
    // 1_000,
    // 10_000,
    // 100_000,
    // 1_000_000,
    // 2_000_000,
    // 5_000_000,
    10_000_000,
    // 20_000_000,
    // 50_000_000,
    // 100_000_000,
];

pub(crate) const S_STRATEGIES: [CRUDProtocol; 11] = [
    MonoWriter,
    LockCoupling,

    orwc_attempts(0),
    orwc_attempts(1),
    orwc_attempts(4),
    orwc_attempts(16),
    orwc_attempts(64),
    orwc_attempts(1024),

    // lightweight_hybrid_lock_read_attempts(0), // only relevant in contented workloads, i.e. WRITE+READ
    // lightweight_hybrid_lock_read_attempts(1),
    // lightweight_hybrid_lock_read_attempts(4),
    // lightweight_hybrid_lock_read_attempts(16),
    // lightweight_hybrid_lock_read_attempts(64),
    // lightweight_hybrid_lock_read_attempts(1024),

    olc(),
    lightweight_hybrid_lock_unlimited(),

    hybrid_lock()
];

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_with(ls, Key::MIN, Key::MAX, inc_key, dec_key);

pub fn log_debug_ln(s: String) {
    println!("> {}", s.replace("\n", "\n>"))
}

pub fn log_debug(s: String) {
    print!("> {}", s.replace("\n", "\n>"))
}

pub fn simple_test() {
    const INSERT: fn(u64) -> CRUDOperation<Key, Payload> = |k: Key|
        CRUDOperation::Insert(k, k as _);

    const UPDATE: fn(u64) -> CRUDOperation<Key, Payload> = |k: Key|
        CRUDOperation::Update(k, k as _);

    let _keys_insert_org = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    let keys_insert_org: Vec<Key> = vec![
        8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18, ];
    //  3, 9, 5, 2, 13, 40, 38, 41, 27, 16, 28, 42, 1, 43, 23, 26,
    // 44, 17, 29, 39, 20, 6, 4, 7, 30, 21, 35, 8];

    // let mut rand = rand::thread_rng();
    // let mut keys_insert = gen_rand_data(1_000);
    //
    // let dups = rand.next_u32().min(keys_insert.len() as _) as usize;
    // keys_insert.extend(keys_insert.get(..dups).unwrap().to_vec());
    // let mut rng = thread_rng();
    // keys_insert.shuffle(&mut rng);

    let mut already_used: Vec<Key> = vec![];
    let keys_insert = keys_insert_org
        .iter()
        .map(|key| if already_used.contains(key) {
            UPDATE(*key)
        } else {
            already_used.push(*key);
            INSERT(*key)
        }).collect::<Vec<_>>();


    let tree = MAKE_INDEX(
        LockingStrategy::MonoWriter);
    let mut search_queries = vec![];

    for (i, tx) in keys_insert.into_iter().enumerate() {
        log_debug_ln(format!("# {}", i + 1));
        log_debug_ln(format!("############################################\
        ###########################################################"));

        let key = match tree.dispatch(tx) {
            CRUDOperationResult::Inserted(key) => {
                log_debug_ln(format!("Ingest: {}", CRUDOperationResult::<Key, Payload>::Inserted(key)));
                key
            }
            CRUDOperationResult::Updated(key, payload) => {
                log_debug_ln(format!("Ingest: {}", CRUDOperationResult::<Key, Payload>::Updated(key, payload)));
                key
            }
            joe => panic!("Sleepy Joe -> TransactionResult::{}", joe)
        };

        let search = vec![
            CRUDOperation::Point(key),
            CRUDOperation::Point(key),
        ];

        search_queries.push(search.clone());
        search.into_iter().for_each(|query| match tree.dispatch(query.clone()) {
            CRUDOperationResult::Error =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::Error),
            CRUDOperationResult::MatchedRecords(records) if records.len() != 1 =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::Error),
            CRUDOperationResult::MatchedRecord(None) =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::MatchedRecord(None)),
            result =>
                log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
        });
        log_debug_ln(format!("##################################################################################\
        ######################\n"));
    }

    log_debug_ln(format!("--------------------------------\
    ------------------------------------------------------------------------"));
    log_debug_ln(format!("----------------------------------\
    ----------------------------------------------------------------------"));
    log_debug_ln(format!("\n############ Query All via Searches ############\n"));
    for (s, chunk) in search_queries.into_iter().enumerate() {
        log_debug_ln(format!("----------------------------------\
        ----------------------------------------------------------------------"));
        log_debug_ln(format!("\t# [{}]", s));
        // if s == 42 {
        //     let x = 31;
        // }
        for query in chunk {
            // if let Transaction::ExactSearchLatest(..) = operation {
            //     continue
            // }
            match tree.dispatch(query.clone()) {
                CRUDOperationResult::Error =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::Error),
                CRUDOperationResult::MatchedRecords(records) if records.len() != 1 =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::Error),
                CRUDOperationResult::MatchedRecord(None) =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::MatchedRecord(None)),
                result =>
                    log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
            }
        }
        log_debug_ln(format!("----------------------------------------------------------\
        ----------------------------------------------\n"));
    }

    show_alignment_bsz();

    let range = Interval::new(
        18,
        36,
    );

    let matches = keys_insert_org
        .into_iter()
        .filter(|k| range.contains(*k))
        .unique();

    let results
        = tree.dispatch(CRUDOperation::Range(range.clone()));

    log_debug_ln(format!("Results of Range Query:\n{}\n\nExpected: \t{}\nFound: \t\t{}\nRange: {}", results, matches.count(), match results {
        CRUDOperationResult::MatchedRecords(ref records) => records.len(),
        _ => 0
    }, range));

    log_debug_ln(format!("Printing Tree:\n"));
    level_order(tree.root.block.clone());
    // json_index(&tree, "simple_tree.json");
}

pub fn gen_rand_data(n: usize) -> Vec<Key> {
    let mut nums = HashSet::new();
    let mut rand = rand::thread_rng();
    loop {
        let next = rand.next_u64() as Key;
        if !nums.contains(&next) {
            nums.insert(next);
        }

        if nums.len() == n as usize {
            break;
        }
    }
    nums.into_iter().collect::<Vec<_>>()
}

#[inline(always)]
pub fn beast_test2(num_thread: usize, p_index: Tree, t1s: &[u64]) -> u128 {
    let mut data_buff = t1s
        .iter()
        .map(|key| CRUDOperation::Insert(*key, Payload::default()))
        .chunks(t1s.len() / num_thread)
        .into_iter()
        .map(|s| s.into_iter().collect::<Vec<_>>())
        .collect::<Vec<_>>();

    let mut handles
        = Vec::with_capacity(num_thread);

    let start = SystemTime::now();
    for _ in 1..=num_thread {
        let current_chunk
            = data_buff.pop().unwrap();

        let index = p_index.clone();
        handles.push(thread::spawn(move || current_chunk
            .into_iter()
            .for_each(|next_query| match index.dispatch(next_query) { // tree.execute(operation),
                CRUDOperationResult::Inserted(key, ..) |
                CRUDOperationResult::Updated(key, ..) => {
                    if EXE_LOOK_UPS {
                        loop {
                            match index.dispatch(CRUDOperation::Point(key)) {
                                CRUDOperationResult::MatchedRecord(Some(record))
                                if record.key == key => { break; }
                                joe => { //  if !tree.locking_strategy().is_dolos()
                                    log_debug_ln(format!("\nSleepy Joe => Transaction::{} ->",
                                                         CRUDOperation::<_, Payload>::Point(key)));
                                    log_debug_ln(format!("\nTransactionResult::{}", joe));
                                    println!()
                                }
                            };
                        }
                    }
                    if EXE_RANGE_LOOK_UPS {
                        loop {
                            match index.dispatch(CRUDOperation::Range((key..=key).into())) {
                                CRUDOperationResult::MatchedRecords(records)
                                if records.len() != 1 =>
                                    println!("Sleepy Joe => RangeQuery len = {} - {}",
                                             records.len(),
                                             records.iter().join("\n")),
                                CRUDOperationResult::MatchedRecords(ref records)
                                if records[0].key != key => //{}
                                    println!("Sleepy Joe => RangeQuery matched garbage record = {}", records[0]),
                                _ => { break; }
                            };
                        }
                    }
                }
                joey => {
                    log_debug_ln(format!("\n#### Sleepy Joe => Transaction ERROR: {}", joey));
                    panic!()
                }
            })));
    }

    handles
        .into_iter()
        .for_each(|handle| handle
            .join()
            .unwrap());

    SystemTime::now().duration_since(start).unwrap().as_millis()
}

#[inline(always)]
pub fn beast_test(num_thread: usize, index: Tree, t1s: &[u64], log: bool) -> u128 {
    let ls = index.as_index().locking_strategy.clone();
    let time = beast_test2(num_thread, index.clone(), t1s);
    if log {
        print!(",{}", ls);
    }

    time
}

pub fn level_order<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display>(root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
{
    let mut queue = VecDeque::new();
    queue.push_back(root);

    while !queue.is_empty() {
        let next = queue.pop_front().unwrap();

        match next.unsafe_borrow().as_ref() {
            Node::Index(index_page) =>
                println!("id: {}, Index(keys: {}, children: {})",
                         next.unsafe_borrow().block_id(),
                         index_page.keys()
                             .iter()
                             .join(","),
                         index_page.children()
                             .iter()
                             .map(|b| {
                                 queue.push_back(b.clone());
                                 b.unsafe_borrow().block_id()
                             })
                             .join(",")),
            Node::Leaf(leaf_page) =>
                println!("id: {}, Leaf({})",
                         next.unsafe_borrow().block_id(),
                         leaf_page.as_records().iter().join(","))
        }
    }
}

pub fn simple_test2() {
    let singled_versioned_index = MAKE_INDEX(LockingStrategy::MonoWriter);

    for key in 1..=10_000 as Key {
        singled_versioned_index.dispatch(CRUDOperation::Insert(key, key as f64));
    }

    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
}

pub fn format_insertions(i: Key) -> String {
    if i % 1_000_000_000 == 0 {
        format!("{} B", i / 1_000_000_000)
    } else if i % 1_000_000 == 0 {
        format!("{} Mio", i / 1_000_000)
    } else if i % 1_000 == 0 {
        format!("{} K", i / 1_000)
    } else {
        i.to_string()
    }
}

pub trait ToGigs {
    fn gigs(self) -> u64;
}

/// Implements the converter method.
impl ToGigs for u64 {
    fn gigs(self) -> u64 {
        self / 1024 / 1024 / 1024
    }
}

pub fn get_system_info() -> String {
    use sysinfo::{NetworkExt, NetworksExt, ProcessExt, SystemExt};

    let mut sys = System::new_all();
    sys.refresh_all();

    let mut system_info = String::new();
    system_info.push_str("# Components temperature:\n");
    let components = sys.components();
    if components.is_empty() {
        system_info.push_str("\t- Error: Couldn't retrieve components information!\n");
    }

    for component in components {
        system_info.push_str(format!("\t- {:?}\n", component).as_str());
    }

    system_info.push_str("\n# System information\n");
    let boot_time = sys.boot_time();
    system_info.push_str(format!("\t- System booted at {} seconds\n", boot_time).as_str());
    let up_time = sys.uptime();
    system_info.push_str(format!("\t- System running since {} seconds\n", up_time).as_str());

    let load_avg = sys.load_average();
    system_info.push_str(format!("\t- System load_avg one minute = {}\n", load_avg.one).as_str());
    system_info.push_str(format!("\t- System load_avg five minutes = {}\n", load_avg.five).as_str());
    system_info.push_str(format!("\t- System load_avg fifteen minutes = {}\n", load_avg.fifteen).as_str());

    system_info.push_str(format!("\t- System name = {:?}\n", sys.name().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System kernel version = {:?}\n", sys.kernel_version().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System OS version = {:?}\n", sys.os_version().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System host name = {:?}\n", sys.host_name().unwrap_or_default()).as_str());

    for user in sys.users() {
        system_info.push_str(format!("\t- User name = {}, groups = {:?}\n", user.name(), user.groups()).as_str());
    }

    let cpuid = raw_cpuid::CpuId::new();
    system_info.push_str("\n# CPU information:\n");
    system_info.push_str(
        format!("\t- Vendor: {}\n",
                cpuid.get_vendor_info()
                    .as_ref()
                    .map_or_else(|| "\t- unknown", |vf| vf.as_str())
        ).as_str());

    system_info.push_str(
        format!("\t- Cores/threads: {}/{}\n", num_cpus::get_physical(), num_cpus::get()).as_str());
    system_info.push_str(
        format!("\t- APIC ID: {}\n",
                cpuid.get_feature_info()
                    .as_ref()
                    .map_or_else(|| String::from("\t- n/a"), |finfo|
                        format!("{}", finfo.initial_local_apic_id()))
        ).as_str());

    // 10.12.8.1 Consistency of APIC IDs and CPUID:
    // "Initial APIC ID (CPUID.01H:EBX[31:24]) is always equal to CPUID.0BH:EDX[7:0]."
    system_info.push_str(
        format!("\t- x2APIC ID: {}\n",
                cpuid.get_extended_topology_info()
                    .map_or_else(|| String::from("n/a"), |mut topiter|
                        format!("{}", match topiter.next() {
                            None => "n/a".to_string(),
                            Some(ref etl) => etl.x2apic_id().to_string()
                        }),
                    )
        ).as_str());

    system_info.push_str(cpuid.get_feature_info().as_ref().map_or_else(
        || format!("\t- Family: {}\n\t- Extended Family: {}\n\t- Model: {}\n\t- Extended Model: {}\n\t- Stepping: {}\n\t- Brand Index: {}\n", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"),
        |finfo|
            format!("\t- Family: {}\n\t- Extended Family: {}\n\t- Model: {}\n\t- Extended Model: {}\n\t- Stepping: {}\n\t- Brand Index: {}\n",
                    finfo.family_id(),
                    finfo.extended_family_id(),
                    finfo.model_id(),
                    finfo.extended_model_id(),
                    finfo.stepping_id(),
                    finfo.brand_index()),
    ).as_str());

    system_info.push_str(format!(
        "\t- Serial#: {}\n",
        cpuid.get_processor_serial().as_ref().map_or_else(
            || String::from("n/a"),
            |serial_info| format!("{}", serial_info.serial()),
        )
    ).as_str());

    let mut features = Vec::with_capacity(80);
    cpuid.get_feature_info().map(|finfo| {
        if finfo.has_sse3() {
            features.push("sse3")
        }
        if finfo.has_pclmulqdq() {
            features.push("pclmulqdq")
        }
        if finfo.has_ds_area() {
            features.push("ds_area")
        }
        if finfo.has_monitor_mwait() {
            features.push("monitor_mwait")
        }
        if finfo.has_cpl() {
            features.push("cpl")
        }
        if finfo.has_vmx() {
            features.push("vmx")
        }
        if finfo.has_smx() {
            features.push("smx")
        }
        if finfo.has_eist() {
            features.push("eist")
        }
        if finfo.has_tm2() {
            features.push("tm2")
        }
        if finfo.has_ssse3() {
            features.push("ssse3")
        }
        if finfo.has_cnxtid() {
            features.push("cnxtid")
        }
        if finfo.has_fma() {
            features.push("fma")
        }
        if finfo.has_cmpxchg16b() {
            features.push("cmpxchg16b")
        }
        if finfo.has_pdcm() {
            features.push("pdcm")
        }
        if finfo.has_pcid() {
            features.push("pcid")
        }
        if finfo.has_dca() {
            features.push("dca")
        }
        if finfo.has_sse41() {
            features.push("sse41")
        }
        if finfo.has_sse42() {
            features.push("sse42")
        }
        if finfo.has_x2apic() {
            features.push("x2apic")
        }
        if finfo.has_movbe() {
            features.push("movbe")
        }
        if finfo.has_popcnt() {
            features.push("popcnt")
        }
        if finfo.has_tsc_deadline() {
            features.push("tsc_deadline")
        }
        if finfo.has_aesni() {
            features.push("aesni")
        }
        if finfo.has_xsave() {
            features.push("xsave")
        }
        if finfo.has_oxsave() {
            features.push("oxsave")
        }
        if finfo.has_avx() {
            features.push("avx")
        }
        if finfo.has_f16c() {
            features.push("f16c")
        }
        if finfo.has_rdrand() {
            features.push("rdrand")
        }
        if finfo.has_fpu() {
            features.push("fpu")
        }
        if finfo.has_vme() {
            features.push("vme")
        }
        if finfo.has_de() {
            features.push("de")
        }
        if finfo.has_pse() {
            features.push("pse")
        }
        if finfo.has_tsc() {
            features.push("tsc")
        }
        if finfo.has_msr() {
            features.push("msr")
        }
        if finfo.has_pae() {
            features.push("pae")
        }
        if finfo.has_mce() {
            features.push("mce")
        }
        if finfo.has_cmpxchg8b() {
            features.push("cmpxchg8b")
        }
        if finfo.has_apic() {
            features.push("apic")
        }
        if finfo.has_sysenter_sysexit() {
            features.push("sysenter_sysexit")
        }
        if finfo.has_mtrr() {
            features.push("mtrr")
        }
        if finfo.has_pge() {
            features.push("pge")
        }
        if finfo.has_mca() {
            features.push("mca")
        }
        if finfo.has_cmov() {
            features.push("cmov")
        }
        if finfo.has_pat() {
            features.push("pat")
        }
        if finfo.has_pse36() {
            features.push("pse36")
        }
        if finfo.has_psn() {
            features.push("psn")
        }
        if finfo.has_clflush() {
            features.push("clflush")
        }
        if finfo.has_ds() {
            features.push("ds")
        }
        if finfo.has_acpi() {
            features.push("acpi")
        }
        if finfo.has_mmx() {
            features.push("mmx")
        }
        if finfo.has_fxsave_fxstor() {
            features.push("fxsave_fxstor")
        }
        if finfo.has_sse() {
            features.push("sse")
        }
        if finfo.has_sse2() {
            features.push("sse2")
        }
        if finfo.has_ss() {
            features.push("ss")
        }
        if finfo.has_htt() {
            features.push("htt")
        }
        if finfo.has_tm() {
            features.push("tm")
        }
        if finfo.has_pbe() {
            features.push("pbe")
        }
    });
    cpuid.get_extended_feature_info().map(|finfo| {
        if finfo.has_bmi1() {
            features.push("bmi1")
        }
        if finfo.has_hle() {
            features.push("hle")
        }
        if finfo.has_avx2() {
            features.push("avx2")
        }
        if finfo.has_fdp() {
            features.push("fdp")
        }
        if finfo.has_smep() {
            features.push("smep")
        }
        if finfo.has_bmi2() {
            features.push("bmi2")
        }
        if finfo.has_rep_movsb_stosb() {
            features.push("rep_movsb_stosb")
        }
        if finfo.has_invpcid() {
            features.push("invpcid")
        }
        if finfo.has_rtm() {
            features.push("rtm")
        }
        if finfo.has_rdtm() {
            features.push("rdtm")
        }
        if finfo.has_fpu_cs_ds_deprecated() {
            features.push("fpu_cs_ds_deprecated")
        }
        if finfo.has_mpx() {
            features.push("mpx")
        }
        if finfo.has_rdta() {
            features.push("rdta")
        }
        if finfo.has_rdseed() {
            features.push("rdseed")
        }
        if finfo.has_adx() {
            features.push("adx")
        }
        if finfo.has_smap() {
            features.push("smap")
        }
        if finfo.has_clflushopt() {
            features.push("clflushopt")
        }
        if finfo.has_processor_trace() {
            features.push("processor_trace")
        }
        if finfo.has_sha() {
            features.push("sha")
        }
        if finfo.has_sgx() {
            features.push("sgx")
        }
        if finfo.has_avx512f() {
            features.push("avx512f")
        }
        if finfo.has_avx512dq() {
            features.push("avx512dq")
        }
        if finfo.has_avx512_ifma() {
            features.push("avx512_ifma")
        }
        if finfo.has_avx512pf() {
            features.push("avx512pf")
        }
        if finfo.has_avx512er() {
            features.push("avx512er")
        }
        if finfo.has_avx512cd() {
            features.push("avx512cd")
        }
        if finfo.has_avx512bw() {
            features.push("avx512bw")
        }
        if finfo.has_avx512vl() {
            features.push("avx512vl")
        }
        if finfo.has_clwb() {
            features.push("clwb")
        }
        if finfo.has_prefetchwt1() {
            features.push("prefetchwt1")
        }
        if finfo.has_umip() {
            features.push("umip")
        }
        if finfo.has_pku() {
            features.push("pku")
        }
        if finfo.has_ospke() {
            features.push("ospke")
        }
        if finfo.has_rdpid() {
            features.push("rdpid")
        }
        if finfo.has_sgx_lc() {
            features.push("sgx_lc")
        }
    });
    system_info.push_str("\t- ");
    system_info.push_str(features.join(" ").as_str());
    system_info.push_str("\n");

    system_info.push_str("\n# System memory:\n");
    system_info.push_str(format!("\t- Used memory : {} KB\n", sys.used_memory()).as_str());
    system_info.push_str(format!("\t- Total memory: {} KB\n", sys.total_memory()).as_str());
    system_info.push_str(format!("\t- Used swap   : {} KB\n", sys.used_swap()).as_str());
    system_info.push_str(format!("\t- Total swap  : {} KB\n", sys.total_swap()).as_str());

    let mut disks = sys.disks();

    system_info.push_str(format!("\n# System Disks: {} disks installed\n", disks.len()).as_str());
    for (index, disk) in disks.iter().enumerate() {
        system_info.push_str(format!("# [{}] - Disk name: {:?}\n\t\
        - type = {:?}\n\t\
        - file system = {}\n\t\
        - total space = {} GB\n\t\
        - free space = {} GB\n\t\
        - mount point = {:?}\n\t\
        - removable = {}\n",
                                     index,
                                     disk.name(),
                                     disk.kind(),
                                     disk.file_system().into_iter().map(|b| char::from(*b)).collect::<String>(),
                                     disk.total_space().gigs(),
                                     disk.available_space().gigs(),
                                     disk.mount_point().as_os_str(),
                                     disk.is_removable()
        ).as_str());
    }

    let networks = sys.networks();
    system_info.push_str(format!("\n# System Networks: {} networks installed\n", networks.iter().count()).as_str());
    for (index, (interface_name, data)) in networks.iter().enumerate() {
        system_info.push_str(format!("# [{}] - Interface name: {}\n\t\
        - received = {}\n\t\
        - errors_on_received = {}\n\t\
        - total_received = {}\n\t\
        - packets_received = {}\n\t\
        - total_packets_received = {}\n\t\
        - total_errors_on_received = {}\n\t\
        - transmitted = {}\n\t\
        - errors_on_transmitted = {}\n\t\
        - total_transmitted = {}\n\t\
        - packets_transmitted = {}\n\t\
        - total_packets_transmitted = {}\n\t\
        - total_errors_on_transmitted = {}\n",
                                     index,
                                     interface_name,
                                     data.received(),
                                     data.errors_on_received(),
                                     data.total_received(),
                                     data.packets_received(),
                                     data.total_packets_received(),
                                     data.total_errors_on_received(),
                                     data.transmitted(),
                                     data.errors_on_transmitted(),
                                     data.total_transmitted(),
                                     data.packets_transmitted(),
                                     data.total_packets_transmitted(),
                                     data.total_errors_on_transmitted()).as_str());
    }

    system_info
}

pub fn create_filter_params(params: &str) -> (Vec<usize>, Vec<Key>, Vec<CRUDProtocol>) {
    let mut p = params.split("+");
    let inserts = serde_json::from_str::<Vec<Key>>(p.next().unwrap_or(""))
        .unwrap_or(S_INSERTIONS.to_vec());

    let mut crud_str
        = p.next().unwrap_or_default().to_string();

    if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
        crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
    }
    if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
        crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
    }

    let threads
        = serde_json::from_str::<Vec<usize>>(p.next().unwrap_or_default())
        .unwrap_or(S_THREADS_CPU.to_vec());

    let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
        .unwrap_or(S_STRATEGIES.to_vec());

    (threads, inserts, crud)
}

pub fn do_tests() {
    let mut args: Vec<String>
        = env::args().collect();

    if args.len() > 1 {
        let raw = args
            .pop()
            .unwrap()
            .split_whitespace()
            .collect::<String>();

        let (params, command) = if raw.contains("=") {
            let mut command_salad = raw.split("=").collect::<Vec<_>>();
            (command_salad.pop().unwrap(), command_salad.pop().unwrap())
        } else {
            ("", raw.as_str())
        };

        match command {
            "all" => experiment(S_THREADS_CPU.to_vec(),
                                S_INSERTIONS.as_slice(),
                                S_STRATEGIES.as_slice()),
            "t1" => println!("Time = {}ms",
                             beast_test(24, TREE(MonoWriter), gen_rand_data(200_000).as_slice(), true)),
            "t2" => println!("Time = {}ms",
                             beast_test(24, TREE(olc()), gen_rand_data(20_000_000).as_slice(), true)),
            "crud_protocol" | "crud_protocols" | "crud" | "cruds" | "protocol" | "protocols" =>
                println!("{}", S_STRATEGIES
                    .as_slice()
                    .iter()
                    .map(|s| format!("Name: \t`{}`\nObject: `{}`",
                                     s,
                                     serde_json::to_string(s).unwrap()))
                    .join("\n******************************************************************\n")),
            "info" | "system" | "sys" => println!("{}", get_system_info()),
            "make_splash" | "splash" =>
                make_splash(),
            "yield_enabled" | "yield" =>
                println!("yield_enabled: {}", ENABLE_YIELD),
            "cpu_cores" | "cpu_threads" | "cpu" =>
                println!("Cores/Threads: {}/{}", num_cpus::get_physical(), num_cpus::get()),
            "simple_test" | "st" =>
                simple_test(),
            "create" | "c" => {
                let (threads, inserts, crud)
                    = create_filter_params(params);

                experiment(threads,
                           inserts.as_slice(),
                           crud.as_slice())
            }
            "update_read" | "ur" => { //update=
                // tree_records+
                // update_records+
                // [CRUD,..]+
                // [t0,..]

                log_debug_ln(format!("Running `{}={}`", command, params));

                let mut params
                    = params.split("+");

                let tree_records
                    = params.next().unwrap().parse::<usize>().unwrap();

                let update_records
                    = params.next().unwrap().parse::<f32>().unwrap();

                let mut crud_str
                    = params.next().unwrap_or_default().to_string();

                let threads
                    = serde_json::from_str::<Vec<usize>>(params.next().unwrap_or_default())
                    .unwrap_or(S_THREADS_CPU.to_vec());

                if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
                    crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
                }

                if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
                    crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
                }

                let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
                    .unwrap_or(S_STRATEGIES.to_vec());

                log_debug_ln(format!("CRUD = `{}` ", crud.as_slice().iter().join(",")));
                log_debug_ln(format!("Threads = `{}` ", threads.as_slice().iter().join(",")));

                let update_records
                    = (update_records * tree_records as f32) as usize;

                log_debug_ln(format!("Records = `{}`, Updates = `{}` ",
                                     format_insertions(tree_records as _),
                                     format_insertions(update_records as _)));

                let data_file
                    = data_file_name(tree_records);

                let read_file
                    = read_data_file_name(tree_records, update_records);

                let from_file = path::Path::new(data_file.as_str())
                    .exists() && path::Path::new(read_file.as_str())
                    .exists();

                let (create_data, read_data) = if from_file {
                    log_debug_ln(format!("Using `{}` for data, `{}` for reads", data_file, read_file));

                    (serde_json::from_str::<Vec<Key>>(fs::read_to_string(data_file).unwrap()
                        .as_str()
                    ).unwrap(), serde_json::from_str::<Vec<Key>>(fs::read_to_string(read_file).unwrap()
                        .as_str()
                    ).unwrap())
                } else {
                    log_debug_ln(format!("Generating `{}` for data", data_file));

                    let c_data = gen_rand_data(tree_records);

                    let mut read_data
                        = (0 as Key..tree_records as Key).collect::<Vec<_>>();

                    read_data.shuffle(&mut rand::thread_rng());
                    read_data.truncate(update_records);

                    read_data
                        .iter_mut()
                        .for_each(|index| *index = c_data[(*index) as usize]);

                    fs::write(data_file, serde_json::to_string(c_data.as_slice()).unwrap())
                        .unwrap();

                    fs::write(read_file, serde_json::to_string(read_data.as_slice()).unwrap())
                        .unwrap();

                    (c_data, read_data)
                };

                crud.into_iter().for_each(|crud| unsafe {
                    log_debug_ln("Creating index...".to_string());
                    let mut index = TREE(crud.clone());

                    let create_time = if crud.is_mono_writer() {
                        beast_test(1, index.clone(), create_data.as_slice(), false)
                    } else {
                        beast_test(4, index.clone(), create_data.as_slice(), false)
                    };

                    log_debug_ln(format!("Created index in `{}` ms", create_time));

                    let read_data: &'static [_] = unsafe { mem::transmute(read_data.as_slice()) };

                    log_debug_ln("UPDATE + READ BENCHMARK; Each Thread = [Updater Thread + Reader Thread]".to_string());
                    println!("Locking Strategy,Threads,Time");
                    threads.iter().for_each(|spawns| unsafe {
                        if crud.is_mono_writer() {
                            let start = SystemTime::now();
                            (0..=*spawns).map(|_| {
                                let i1 = index.clone();
                                let i2 = index.clone();
                                [thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error =
                                            i1.dispatch(CRUDOperation::Point(*read_key))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                }), thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error
                                            = i2.dispatch(
                                            CRUDOperation::Update(*read_key, Payload::default()))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                })]
                            }).collect::<Vec<_>>()
                                .into_iter()
                                .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                            println!("{},{},{}", crud, *spawns,
                                     SystemTime::now().duration_since(start).unwrap().as_millis());
                        } else {
                            let read_data = read_data.clone();
                            let index_r: &'static INDEX = mem::transmute(&index);
                            let start = SystemTime::now();
                            (0..=*spawns).map(|_| {
                                [thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error =
                                            index_r.dispatch(CRUDOperation::Point(*read_key))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                }), thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error
                                            = index_r.dispatch(
                                            CRUDOperation::Update(*read_key, Payload::default()))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                })]
                            }).collect::<Vec<_>>()
                                .into_iter()
                                .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                            println!("{},{},{}", crud, *spawns,
                                     SystemTime::now().duration_since(start).unwrap().as_millis());
                        }
                    });
                });
            }
            "generate" | "gen" => fs::write(
                data_file_name(params.parse::<usize>().unwrap()),
                serde_json::to_string(
                    gen_rand_data(params.parse::<usize>().unwrap()).as_slice()).unwrap(),
            ).unwrap(),
            "block_alignment" | "bsz_aln" | "alignment" | "aln" | "block" | "bsz" =>
                show_alignment_bsz(),
            "hardware_lock_elision" | "hle" =>
                println!("OLC hardware_lock_elision: {}", hle()),
            "x86_64" | "x86" =>
                println!("x86_64 or x86: {}", cfg!(any(target_arch = "x86", target_arch = "x86_64"))),
            _ => make_splash(),
        }
    } else {
        make_splash()
    }
}

pub fn data_file_name(n_records: usize) -> String {
    format!("create_{}.create", format_insertions(n_records as _))
}

pub fn read_data_file_name(n_records: usize, read_records: usize) -> String {
    format!("{}__read_{}.read",
            data_file_name(n_records).replace(".create", ""),
            format_insertions(read_records as _))
}

pub fn show_alignment_bsz() {
    log_debug_ln(format!("\t- Block Size: \t\t{} bytes\n\t\
        - Block Align-Size: \t{} bytes\n\t\
        - Block/Delta: \t\t{}/{} bytes\n\t\
        - Num Keys: \t\t{}\n\t\
        - Fan Out: \t\t{}\n\t\
        - Num Records: \t\t{}\n",
                         BSZ_BASE,
                         bsz_alignment::<Key, Payload>(),
                         mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         BSZ_BASE - mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         FAN_OUT - 1,
                         FAN_OUT,
                         NUM_RECORDS)
    );
}

pub fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        } else {
            "NO HTL"
        }
    } else {
        "OFF   "
    }
}

pub fn experiment(threads_cpu: Vec<usize>,
                  insertions: &[Key],
                  strategies: &[LockingStrategy])
{
    // if CPU_THREADS {
    //     let cpu = num_cpus::get();
    //     threads_cpu.truncate(threads_cpu
    //         .iter()
    //         .enumerate()
    //         .find(|(_, t)| **t > cpu)
    //         .unwrap()
    //         .0)
    // }

    println!("Number Insertions,Number Threads,Locking Strategy,Time,Fan Out,Leaf Records,Block Size");

    for insertion_n in insertions {
        let t1s = gen_rand_data(*insertion_n as usize);
        for num_threads in threads_cpu.iter() {
            // if *num_threads > t1s.len() {
            //     continue;
            // }

            for ls in strategies {
                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let time = beast_test2(
                    *num_threads,
                    TREE(ls.clone()),
                    t1s.as_slice());

                print!(",{}", time);
                print!(",{}", FAN_OUT);
                print!(",{}", NUM_RECORDS);
                println!(",{}", BSZ_BASE);
            }
        }
    }
}

pub fn start_paper_tests() {
    for n in S_INSERTIONS {
        let file_suffix = format_insertions(n as _);
        let create_file = format!("create_{}.bin", file_suffix);
        let create_file = create_file.as_str();

        let scan_file = format!("scan_{}.bin", file_suffix);
        let scan_file = scan_file.as_str();

        if !Path::new(create_file).exists() || !Path::new(scan_file).exists() {
            let t1s
                = gen_rand_data(n as usize);

            let mut scans = t1s.clone();
            scans.shuffle(&mut rand::thread_rng());

            fs::write(create_file, unsafe {
                std::slice::from_raw_parts(t1s.as_ptr() as _, t1s.len() * mem::size_of::<Key>())
            }).unwrap();

            fs::write(scan_file, unsafe {
                std::slice::from_raw_parts(scans.as_ptr() as _, scans.len() * mem::size_of::<Key>())
            }).unwrap();
        };
    }

    for n in S_INSERTIONS {
        let file_suffix = format_insertions(n as _);
        let create_file = format!("create_{}.bin", file_suffix);
        let create_file = create_file.as_str();

        let scan_file = format!("scan_{}.bin", file_suffix);
        let scan_file = scan_file.as_str();

        unsafe {
            let mut create = fs::read(create_file).unwrap();
            create.set_len(create.len() / 8);

            let mut scan = fs::read(scan_file).unwrap();
            scan.set_len(scan.len() / 8);

            let create: Vec<Key> = mem::transmute(create);
            let scan: Vec<Key> = mem::transmute(scan);

            create_scan_test(create.as_slice(), scan.as_slice());
            update_test(create.as_slice(), scan.as_slice());
        }
    }
}

fn update_test(t1s: &[Key], updates: &[Key]) {
    let threads_cpu
        = S_THREADS_CPU.to_vec();

    let strategies
        = S_STRATEGIES.to_vec();

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let index = TREE(ls.clone());
            let _ = beast_test(
                *num_threads,
                index.clone(),
                t1s, true);

            let chunk_size = updates.len() / *num_threads;
            let mut slices = (0..*num_threads).map(|i| unsafe {
                std::slice::from_raw_parts(
                    updates.as_ptr().add(i * chunk_size),
                    chunk_size)
            }).collect::<VecDeque<_>>();

            let start = SystemTime::now();
            let update_handles = (0..*num_threads).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                let index
                    = index.clone();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Update(*key, Payload::default())) {
                            CRUDOperationResult::Updated(..) => {}
                            CRUDOperationResult::Error => log_debug(format!("Not found key = {}", key)),
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }).collect::<Vec<_>>();
            update_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());

            let update_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            print!(",{}", update_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            println!(",{}", BSZ_BASE);
        }
    }
}

fn create_scan_test(t1s: &[Key], scans: &[Key]) {
    let threads_cpu
        = S_THREADS_CPU.to_vec();

    let strategies
        = S_STRATEGIES.to_vec();

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let index = TREE(ls.clone());
            let create_time = beast_test(
                *num_threads,
                index.clone(),
                t1s, true);

            print!(",{}", create_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            print!(",{}", BSZ_BASE);

            let chunk_size = scans.len() / *num_threads;
            let mut slices = (0..*num_threads).map(|i| unsafe {
                std::slice::from_raw_parts(
                    scans.as_ptr().add(i * chunk_size),
                    chunk_size)
            }).collect::<VecDeque<_>>();

            let start = SystemTime::now();
            let read_handles = (0..*num_threads).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                let index
                    = index.clone();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Point(*key)) {
                            CRUDOperationResult::MatchedRecord(..) => {}
                            CRUDOperationResult::Error => log_debug_ln(format!("Not found key = {}", key)),
                            cor =>
                                log_debug_ln(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }).collect::<Vec<_>>();
            read_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());


            let read_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            println!(",{}", read_time);
        }
    }
}