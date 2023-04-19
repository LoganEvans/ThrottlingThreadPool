#include "worker.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "epoch.h"
#include "executor.h"

namespace theta {

Worker::Worker(TaskQueue* run_queue, NicePriority priority)
    : run_queue_(run_queue), thread_(&Worker::run_loop, this) {
  set_nice_priority(priority);
}

Worker::~Worker() { thread_.join(); }

void Worker::shutdown() {
  run_queue_->shutdown();
}

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

  pthread_setschedparam(get_pthread(), sched_policy, &param);
}

pthread_t Worker::get_pthread() {
  return thread_.native_handle();
}

void Worker::run_loop() {
  while (true) {
    EpochPtr<Task> task = run_queue_->wait_pop();

    if (!task) {
      CHECK(run_queue_->is_shutting_down());
      break;
    }

    task->run();
    task->set_state(Task::State::kFinished);

    // TODO(lpe): After finishing a task, check to see if this worker needs to
    // convert to another priority level. That will happen when the number of
    // workers at certain priority are out of balance due to a task being
    // throttled or promotted while running.

    // TODO(lpe): Set high priority for refilling the queues?
    task->opts().executor()->refill_queues();
  }
}

}  // namespace theta
