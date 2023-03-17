#include "benchmark/benchmark.h"
#include "threadpool.h"

static void BM_Foo(benchmark::State &state) {
  int i = 0;
  for (auto _ : state) {
    i++;
    benchmark::DoNotOptimize(i);
  }
}
BENCHMARK(BM_Foo);

BENCHMARK_MAIN();
