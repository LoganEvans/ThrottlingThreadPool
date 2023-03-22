#include "worker.h"

#include <glog/logging.h>

namespace theta {

Worker::Worker(TaskQueues* queues, TaskQueues::NicePriority priority)
    : queues_(queues), priority_(priority), thread_(&Worker::run_loop, this) {}

Worker::~Worker() { thread_.join(); }

void Worker::shutdown() { shutdown_.store(true, std::memory_order_release); }

TaskQueues::NicePriority Worker::nice_priority() const {
  return priority_.load(std::memory_order_acquire);
}

void Worker::set_nice_priority(TaskQueues::NicePriority priority) {
  priority_.store(priority, std::memory_order_release);
}

void Worker::run_loop() {
  while (true) {
    auto priority = nice_priority();
    Func func = queues_->pop_blocking(priority);

    auto new_priority = nice_priority();
    if (priority != new_priority) {
      // TODO(lpe): This indicates that the thread needs to change its nice
      // value. Afterward, the func should be requeued.
      CHECK(false);
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      break;
    }

    CHECK(func != nullptr);
    func();
  }
}

}  // namespace theta
