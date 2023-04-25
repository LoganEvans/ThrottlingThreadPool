#include <atomic>
#include <memory>
#include <semaphore>

#include "benchmark/benchmark.h"
#include "queue.h"
#include "threadpool.h"

namespace theta {

template <typename QType>
static void BM_queue(benchmark::State &state) {
  static std::atomic<QType *> tq{nullptr};
  static std::atomic<int> workers{0};

  if (state.thread_index() == 0) {
    tq.store(new QType{}, std::memory_order::release);
  }

  workers.fetch_add(1, std::memory_order::acq_rel);
  QType *task_queue{nullptr};
  do {
    task_queue = tq.load(std::memory_order::acquire);
  } while (!task_queue);

  for (auto _ : state) {
    task_queue->push(std::make_unique<Task>(Task::Opts{}));
    task_queue->wait_pop();
  }

  workers.fetch_sub(1, std::memory_order::acq_rel);

  if (state.thread_index() == 0) {
    while (workers.load(std::memory_order::acq_rel)) {
    }
    task_queue->shutdown();
    delete tq.exchange(nullptr);
  }
}

using CustomSemaphore = Semaphore;
BENCHMARK_TEMPLATE(BM_queue, TaskQueue<CustomSemaphore>)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);
BENCHMARK_TEMPLATE(BM_queue, TaskQueue<std::counting_semaphore<100>>)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

template <typename SemaphoreType>
static void BM_semaphore(benchmark::State &state) {
  static SemaphoreType sem{0};

  for (auto _ : state) {
    sem.release();
    semaphoreAcquireKludge(sem);
  }
}

BENCHMARK_TEMPLATE(BM_semaphore, CustomSemaphore)->Threads(1)->Threads(2);
BENCHMARK_TEMPLATE(BM_semaphore, std::counting_semaphore<100>)
    ->Threads(1)
    ->Threads(2);

}  // namespace theta

BENCHMARK_MAIN();
