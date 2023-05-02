#include "worker.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "executor.h"

namespace theta {

Worker::Worker(TaskQueue<>* run_queue)
    : run_queue_(run_queue), thread_(&Worker::run_loop, this) {}

Worker::~Worker() { thread_.join(); }

void Worker::shutdown() {
  run_queue_->shutdown();
}

NicePriority Worker::nice_priority() const {
  return priority_.load(std::memory_order_acquire);
}

void Worker::set_nice_priority(NicePriority priority) {
  static int sched_policy = sched_getscheduler(getpid());

  if (priority_.load(std::memory_order::acquire) == priority) {
    return;
  }

  std::lock_guard lock{priority_mutex_};

  auto old_priority = priority_.exchange(priority, std::memory_order::acq_rel);
  if (old_priority == priority) {
    return;
  }

  sched_param param;
  switch (priority) {
    case NicePriority::kThrottled:
      param.sched_priority = -20;
      break;
    case NicePriority::kPrioritized:
      param.sched_priority = 19;
      break;
    default:
      param.sched_priority = 0;
      break;
  }

  pthread_setschedparam(get_pthread(), sched_policy, &param);
}

pthread_t Worker::get_pthread() {
  return thread_.native_handle();
}

void Worker::run_loop() {
  Task* task{nullptr};
  while (true) {
    fprintf(stderr, "> Worker::run_loop()\n");
    if (!task) {
      task = run_queue_->wait_pop().release();
    }
    if (!task) {
      CHECK(run_queue_->is_shutting_down());
      return;
    }

    auto* executor = task->opts().executor();
    task->set_worker(this);
    Task::run(executor, std::unique_ptr<Task>(task));
    task = nullptr;
    executor->refill_queues(&task);
    fprintf(stderr, "< Worker::run_loop()\n");
  }
}

}  // namespace theta
