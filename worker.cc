#include "worker.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

namespace theta {

Worker::Worker(TaskQueues* queues, TaskQueues::NicePriority priority)
    : queues_(queues), thread_(&Worker::run_loop, this) {
  set_nice_priority(priority);
}

Worker::~Worker() { thread_.join(); }

void Worker::shutdown() { shutdown_.store(true, std::memory_order_release); }

TaskQueues::NicePriority Worker::nice_priority() const {
  return priority_.load(std::memory_order_acquire);
}

void Worker::set_nice_priority(TaskQueues::NicePriority priority) {
  static int sched_policy = sched_getscheduler(getpid());

  priority_.store(priority, std::memory_order_release);

  sched_param param;
  switch (priority) {
    case TaskQueues::NicePriority::kThrottled:
      param.sched_priority = -20;
      break;
    case TaskQueues::NicePriority::kPrioritized:
      param.sched_priority = 19;
      break;
    default:
      param.sched_priority = 0;
      break;
  }

  pthread_setschedparam(thread_.native_handle(), sched_policy, &param);
}

void Worker::run_loop() {
  while (true) {
    auto priority = nice_priority();
    auto task = queues_->pop_blocking(priority);

    auto new_priority = nice_priority();
    if (priority != new_priority) {
      // TODO(lpe): This indicates that the thread needs to change its nice
      // value. Afterward, the func should be requeued.
      CHECK(false);
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      break;
    }

    CHECK(*task);
    task->opts().func()();
  }
}

}  // namespace theta
