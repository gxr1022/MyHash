#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <gflags/gflags.h>
#include "Myhash.h"
#include "client.h"
#include "mm.h"


int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, false);
    Client kv_bench(argc, argv);
    kv_bench.load_and_run();
    return 0;
}




