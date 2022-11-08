#include <iostream>
#include <algorithm>
#include "BTreeOLC.h"
#include "randGen.h"
#include <chrono>
using namespace std::chrono;

const uint64_t N = 100000000;
const uint64_t T = 1;

static void* thread_insert(void* ptr);
static void experiment(int n, int t, uint64_t* all_data);

struct thread_s {
    uint64_t n;
    uint64_t* data;
    pthread_t p_thread;
    btreeolc::BTree<uint64_t, double>* tree;
};

int main(int argc, char** argv) {
    auto n = N;
    auto t = T;

    if(argc == 3) {
        n = std::stol(argv[1]);
        t = std::stol(argv[2]);
    }

    std::cout << "> Generating data, hold on ..." << std::endl;
    auto* all_data = const_cast<uint64_t *>(gen_rand_data(n));
    std::cout << "> Data generation completed" << std::endl;

    std::cout << "Number of Records,Number of Threads,Time" << std::endl;

    int all_t[] = { 1,
                    2,
                    4,
                    5,
                    6,
                    8,
                    10,
                    12,
                    14,
                    15,
                    16,
                    18,
                    20,
                    32,
                    48,
                    64,
                    96,
                    128
    };

    for(int tt : all_t)
        experiment(n, tt, all_data);

    return 0;
}

void experiment(int n, int t, uint64_t* all_data) {
    thread_s threads[128];
    uint64_t relative_n = n / t;

    auto tree = new btreeolc::BTree<uint64_t, double>();

    milliseconds time_start = duration_cast< milliseconds >(
            system_clock::now().time_since_epoch());

    for(int i = 0; i < t; i++) {
        threads[i].data = all_data + (i * relative_n);
        threads[i].n = relative_n;
        threads[i].tree = tree;

        if (pthread_create(&threads[i].p_thread,
                           NULL,
                           &thread_insert,
                           &threads[i]) != 0)
        {
            return;
        }
    }

    for(int i = 0; i < t; i++) {
        pthread_join(threads[i].p_thread, NULL);
    }

    milliseconds time = duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()) - time_start;

    std::cout << n << "," << t << "," << time.count() << std::endl;
}

void* thread_insert(void* ptr) {
    auto* thread_data = (thread_s*) ptr;

    for(int i = 0; i < thread_data->n; i++)
        thread_data->tree->insert(thread_data->data[i], (double) thread_data->data[i]);

    return NULL;
}
