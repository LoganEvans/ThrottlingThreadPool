#include <atomic>
#include <barrier>
#include <memory>
#include <semaphore>

#include "benchmark/benchmark.h"
#include "queue.h"

namespace theta {

template <typename QType, bool kUseTry>
static void producer_consumer(benchmark::State& state, int num_producers,
                              int num_consumers) {
  QType queue{QueueOpts{}};

  std::atomic<bool> done{false};
  int end_sentinel;
  std::mutex mu;

  auto consumer_work = [&]() {
    while (true) {
      int* x = nullptr;
      if constexpr (kUseTry) {
        while (true) {
          auto res = queue.try_pop();
          if (res.has_value()) {
            x = res.value();
            break;
          }
          std::this_thread::yield();
        }
      } else {
        x = queue.pop();
      }
      if (x == &end_sentinel) {
        return;
      }
    }
  };

  std::vector<std::thread> consumers;
  for (int64_t i = 0; i < num_consumers; i++) {
    consumers.push_back(std::thread{consumer_work});
  }

  auto producer_work = [&]() {
    const size_t kBatchSize = 10000;
    while (true) {
      {
        std::lock_guard l{mu};
        if (done.load(std::memory_order::acquire) ||
            !state.KeepRunningBatch(kBatchSize)) {
          done.store(true, std::memory_order::release);
          return;
        }
      }

      int foo;
      for (size_t i = 0; i < kBatchSize; i++) {
        if constexpr (kUseTry) {
          while (!queue.try_push(&foo)) {
            std::this_thread::yield();
          }
        } else {
          queue.push(&foo);
        }
      }
    }
  };

  std::vector<std::thread> producers;
  for (int64_t i = 0; i < num_producers; i++) {
    producers.push_back(std::thread{producer_work});
  }

  for (auto& p : producers) {
    p.join();
  }

  for (int i = 0; i < num_consumers; i++) {
    queue.push(&end_sentinel);
  }

  for (auto& p : consumers) {
    p.join();
  }
}

template <typename QType>
static void BM_multi_producer_single_consumer(benchmark::State& state) {
  producer_consumer<QType, /*kUseTry=*/false>(state, state.range(0), 1);
}
// BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, MPSCQueue<int*>)
//     ->Args({1})
//     ->Args({2})
//     ->Args({4})
//     ->Args({8})
//     ->Args({12})
//     ->Args({24});
BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({12})
    ->Args({24});

template <typename QType>
static void BM_multi_producer_single_consumer_try(benchmark::State& state) {
  producer_consumer<QType, /*kUseTry=*/true>(state, state.range(0), 1);
}
// BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer_try, MPSCQueue<int*>)
//     ->Args({1})
//     ->Args({2})
//     ->Args({4})
//     ->Args({8})
//     ->Args({12})
//     ->Args({24});
BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer_try, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({12})
    ->Args({24});

template <typename QType>
static void BM_multi_producer_multi_consumer_try(benchmark::State& state) {
  producer_consumer<QType, /*kUseTry=*/true>(state, state.range(0),
                                             state.range(0));
}
// BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer_try, MPSCQueue<int*>)
//     ->Args({1})
//     ->Args({2})
//     ->Args({4})
//     ->Args({6})
//     ->Args({8})
//     ->Args({12})
//     ->Args({24});
//
BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer_try, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({6})
    ->Args({8})
    ->Args({12})
    ->Args({24});

template <typename QType>
static void BM_multi_producer_multi_consumer(benchmark::State& state) {
  producer_consumer<QType, /*kUseTry=*/false>(state, state.range(0),
                                              state.range(0));
}
BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({6})
    ->Args({8})
    ->Args({12})
    ->Args({24});

}  // namespace theta

BENCHMARK_MAIN();
