#include <atomic>
#include <memory>
#include <semaphore>

#include "benchmark/benchmark.h"
#include "queue.h"
#include "threadpool.h"

namespace theta {

//static void BM_TaskQueue_counting_semaphore(benchmark::State &state) {
//  using QType = TaskQueue<
//      /*SemaphoreType=*/std::counting_semaphore<100>>;
//  static std::atomic<QType *> tq{nullptr};
//
//  if (state.thread_index() == 0) {
//    tq.store(new QType{}, std::memory_order::release);
//  }
//
//  QType *task_queue{nullptr};
//  do {
//    task_queue = tq.load(std::memory_order::acquire);
//  } while (!task_queue);
//
//  fprintf(stderr, "here\n");
//
//  for (auto _ : state) {
//    task_queue->push(std::make_unique<Task>(Task::Opts{}));
//    task_queue->wait_pop();
//  }
//
//  fprintf(stderr, "there\n");
//
//  if (state.thread_index() == 0) {
//    task_queue->shutdown();
//    delete tq.exchange(nullptr);
//  }
//}
//BENCHMARK(BM_TaskQueue_counting_semaphore)->Threads(10);
//
//static void BM_TaskQueue_custom_semaphore(benchmark::State &state) {
//  using QType = TaskQueue<
//      /*SemaphoreType=*/Semaphore>;
//  static std::atomic<QType *> tq{nullptr};
//
//  if (state.thread_index() == 0) {
//    tq.store(new QType{}, std::memory_order::release);
//  }
//
//  QType *task_queue{nullptr};
//  do {
//    task_queue = tq.load(std::memory_order::acquire);
//  } while (!task_queue);
//
//  for (auto _ : state) {
//    task_queue->push(std::make_unique<Task>(Task::Opts{}));
//    task_queue->wait_pop();
//  }
//
//  if (state.thread_index() == 0) {
//    task_queue->shutdown();
//    delete tq.exchange(nullptr);
//  }
//}
//BENCHMARK(BM_TaskQueue_custom_semaphore)->Threads(10);

template <typename SemaphoreType>
static void BM_semaphore(benchmark::State &state) {
  static SemaphoreType sem{0};

  for (auto _ : state) {
    sem.release();
    sem.acquire();
  }
}

BENCHMARK_TEMPLATE(BM_semaphore, std::counting_semaphore<100>)->Threads(1);

using CustomSemaphore = Semaphore;
BENCHMARK_TEMPLATE(BM_semaphore, CustomSemaphore)->Threads(1)->Threads(10);

}  // namespace theta

BENCHMARK_MAIN();
