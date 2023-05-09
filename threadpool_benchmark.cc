#include <atomic>
#include <memory>
#include <semaphore>

#include "benchmark/benchmark.h"
#include "queue.h"
#include "semaphore.h"
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

static void BM_empty_tasks(benchmark::State &state) {
  Executor executor = ThrottlingThreadpool::getInstance().create(
      Executor::Opts{}
          .set_priority_policy(PriorityPolicy::FIFO)
          .set_thread_weight(state.range(0))
          .set_worker_limit(state.range(0)));

  int jobs_posted{0};
  std::atomic<int> jobs_run{0};

  auto job = std::function<void()>([&]() {
    jobs_run.fetch_add(1, std::memory_order::acq_rel);
  });

  for (auto _ : state) {
    executor.post(job);
    jobs_posted++;

    while (jobs_posted - jobs_run.load(std::memory_order::acquire) > 1000) {
    }
  }

  while (jobs_run.load(std::memory_order::acquire) < jobs_posted) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}
BENCHMARK(BM_empty_tasks)->Args({1})->Args({2})->Args({11})->Args({100});

}  // namespace theta

BENCHMARK_MAIN();
