#include "worker.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "executor.h"

namespace theta {

Worker::Worker(TaskQueues* queues, NicePriority priority)
    : queues_(queues), thread_(&Worker::run_loop, this) {
  set_nice_priority(priority);
}

Worker::~Worker() { thread_.join(); }

void Worker::shutdown() { shutdown_.store(true, std::memory_order_release); }

NicePriority Worker::nice_priority() const {
  return priority_.load(std::memory_order_acquire);
}

void Worker::set_nice_priority(NicePriority priority) {
  static int sched_policy = sched_getscheduler(getpid());

  priority_.store(priority, std::memory_order_release);

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

  pthread_setschedparam(thread_.native_handle(), sched_policy, &param);
}

void Worker::run_loop() {
  std::shared_ptr<Task> task{nullptr};

  while (true) {
    if (!task) {
      auto priority = nice_priority();
      task = queues_->queue(priority)->pop_blocking();
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      break;
    }

    CHECK(task);
    task->run();

    // TODO(lpe): After finishing a task, check to see if this worker needs to
    // convert to another priority level. That will happen when the number of
    // workers at certain priority are out of balance due to a task being
    // throttled or promotted while running.

    auto newTask = task->opts().executor()->pop();
    task = nullptr;

    if (newTask && queues_->queue(newTask->nice_priority())->is_empty()) {
      task = newTask;
      task->set_state(Task::nice2queued(task->nice_priority()));
    } else if (newTask) {
      queues_->push(newTask);
    }
  }
}

}  // namespace theta
