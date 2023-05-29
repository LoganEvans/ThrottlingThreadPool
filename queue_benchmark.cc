#include <atomic>
#include <barrier>
#include <memory>
#include <semaphore>

#include "benchmark/benchmark.h"
#include "queue.h"

namespace theta {

template <typename QType>
static void BM_multi_producer_single_consumer(benchmark::State &state) {
  QType queue{QueueOpts{}};
  std::atomic<bool> done{false};

  auto work = [&]() {
    int foo;
    while (!done.load(std::memory_order::acquire)) {
      queue.push_back(&foo);
    }
  };

  std::vector<std::thread> producers;
  for (int64_t i = 0; i < state.range(0); i++) {
    producers.push_back(std::thread{work});
  }

  for (auto _ : state) {
    queue.pop_front();
  }

  done.store(true, std::memory_order::release);

  for (auto& p : producers) {
    p.join();
  }
}

BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, MPSCQueue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({12})
    ->Args({24});
BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({12})
    ->Args({24});

}  // namespace theta

BENCHMARK_MAIN();
