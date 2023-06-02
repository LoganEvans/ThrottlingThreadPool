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
      queue.try_push_back(&foo);
    }
  };

  std::vector<std::thread> producers;
  for (int64_t i = 0; i < state.range(0); i++) {
    producers.push_back(std::thread{work});
  }

  for (auto _ : state) {
    queue.try_pop_front();
  }

  done.store(true, std::memory_order::release);

  for (auto& p : producers) {
    p.join();
  }
}

//BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, MPSCQueue<int*>)
//    ->Args({1})
//    ->Args({2})
//    ->Args({4})
//    ->Args({8})
//    ->Args({12})
//    ->Args({24});
//BENCHMARK_TEMPLATE(BM_multi_producer_single_consumer, Queue<int*>)
//    ->Args({1})
//    ->Args({2})
//    ->Args({4})
//    ->Args({8})
//    ->Args({12})
//    ->Args({24});

template <typename QType>
static void BM_multi_producer_multi_consumer_try(benchmark::State &state) {
  QType queue{QueueOpts{}};

  std::atomic<bool> done{false};
  int end_sentinel;
  std::mutex mu;

  auto consumer_work = [&]() {
    while (true) {
      auto x = queue.try_pop_front();
      if (x == &end_sentinel) {
        return;
      }
    }
  };

  std::vector<std::thread> consumers;
  for (int64_t i = 0; i < state.range(0); i++) {
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
          while (!queue.try_push_back(&end_sentinel)) {
          }
          return;
        }
      }

      int foo;
      for (size_t i = 0; i < kBatchSize; i++) {
        queue.try_push_back(&foo);
      }
    }
  };

  std::vector<std::thread> producers;
  for (int64_t i = 0; i < state.range(0); i++) {
    producers.push_back(std::thread{producer_work});
  }

  for (auto& p : producers) {
    p.join();
  }

  for (auto& p : consumers) {
    p.join();
  }
}

BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer_try, MPSCQueue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({6})
    ->Args({8})
    ->Args({12})
    ->Args({24});

BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer_try, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({6})
    ->Args({8})
    ->Args({12})
    ->Args({24})
    ;

template <typename QType>
static void BM_multi_producer_multi_consumer(benchmark::State &state) {
  QType queue{QueueOpts{}};

  std::atomic<bool> done{false};
  int end_sentinel;
  std::mutex mu;

  auto consumer_work = [&]() {
    while (true) {
      auto x = queue.pop_front();
      if (x == &end_sentinel) {
        return;
      }
    }
  };

  std::vector<std::thread> consumers;
  for (int64_t i = 0; i < state.range(0); i++) {
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
          queue.push_back(&end_sentinel);
          return;
        }
      }

      int foo;
      for (size_t i = 0; i < kBatchSize; i++) {
        queue.push_back(&foo);
      }
    }
  };

  std::vector<std::thread> producers;
  for (int64_t i = 0; i < state.range(0); i++) {
    producers.push_back(std::thread{producer_work});
  }

  for (auto& p : producers) {
    p.join();
  }

  for (auto& p : consumers) {
    p.join();
  }
}

BENCHMARK_TEMPLATE(BM_multi_producer_multi_consumer, Queue<int*>)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({6})
    ->Args({8})
    ->Args({12})
    ->Args({24})
    ;

}  // namespace theta

BENCHMARK_MAIN();
