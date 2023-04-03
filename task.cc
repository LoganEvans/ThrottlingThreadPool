#include "task.h"

#include <glog/logging.h>

#include "executor.h"

namespace theta {

Task::State Task::state() const {
  std::lock_guard lock{mutex_};
  return state_;
}

void Task::set_state(State state) {
  std::lock_guard lock{mutex_};
  set_state(state, lock);
}

void Task::set_state(State state, const std::lock_guard<std::mutex>&) {
  State old = state_;
  state_ = state;
  printf("> set_state() %d -> %d\n", old, state);

  if (old == State::kCreated && state == State::kQueuedExecutor) {
    opts().executor()->stats()->waiting_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedPrioritized) {
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedThrottled) {
    opts().executor()->stats()->throttled_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedNormal) {
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kQueuedExecutor &&
             state == State::kQueuedPrioritized) {
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kQueuedExecutor &&
             state == State::kQueuedThrottled) {
    opts().executor()->stats()->throttled_delta(1);
  } else if (old == State::kQueuedExecutor && state == State::kQueuedNormal) {
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kQueuedPrioritized &&
             state == State::kRunningPrioritized) {
  } else if (old == State::kQueuedPrioritized &&
             state == State::kQueuedThrottled) {
    opts().executor()->stats()->running_delta(-1);
    opts().executor()->stats()->throttled_delta(1);
  } else if (old == State::kQueuedThrottled &&
             state == State::kQueuedPrioritized) {
    opts().executor()->stats()->throttled_delta(-1);
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kQueuedThrottled &&
             state == State::kRunningThrottled) {
  } else if (old == State::kQueuedThrottled && state == State::kQueuedNormal) {
    opts().executor()->stats()->throttled_delta(-1);
    opts().executor()->stats()->running_delta(1);
  } else if (old == State::kQueuedNormal && state == State::kQueuedThrottled) {
    opts().executor()->stats()->running_delta(-1);
    opts().executor()->stats()->throttled_delta(1);
  } else if (old == State::kQueuedNormal && state == State::kRunningNormal) {
  } else if (old == State::kRunningPrioritized &&
             state == State::kRunningThrottled) {
    opts().executor()->stats()->running_delta(-1);
    opts().executor()->stats()->throttled_delta(1);
  } else if (old == State::kRunningPrioritized && state == State::kFinished) {
    opts().executor()->stats()->running_delta(-1);
    opts().executor()->stats()->finished_delta(1);
  } else if (old == State::kRunningThrottled &&
             state == State::kRunningPrioritized) {
  } else if (old == State::kRunningThrottled &&
             state == State::kRunningNormal) {
  } else if (old == State::kRunningThrottled && state == State::kFinished) {
    opts().executor()->stats()->throttled_delta(-1);
    opts().executor()->stats()->finished_delta(1);
  } else if (old == State::kRunningNormal && state == State::kRunningThrottled) {
  } else if (old == State::kRunningNormal && state == State::kFinished) {
    opts().executor()->stats()->running_delta(-1);
    opts().executor()->stats()->finished_delta(1);
  } else {
    CHECK(false) << "illegal state transition: " << int(old) << " -> "
                 << int(state);
  }

  state_ = state;
}

NicePriority Task::nice_priority() const {
  std::lock_guard lock{mutex_};
  return nice_priority_;
}

void Task::set_nice_priority(NicePriority priority) {
  std::lock_guard lock{mutex_};
  set_nice_priority(priority, lock);
}

void Task::set_nice_priority(NicePriority priority,
                             const std::lock_guard<std::mutex>&) {
  nice_priority_ = priority;

  // TODO(lpe): If there's a worker, possibly change the worker run state.
}

/*static*/
Task::State Task::nice2queued(NicePriority priority) {
  if (priority == NicePriority::kThrottled) {
    return State::kQueuedThrottled;
  } else if (priority == NicePriority::kRunning) {
    return State::kQueuedNormal;
  } else if (priority == NicePriority::kPrioritized) {
    return State::kQueuedPrioritized;
  }

  CHECK(false) << "priority: " << int(priority);
}

/*static*/
Task::State Task::nice2running(NicePriority priority) {
  if (priority == NicePriority::kThrottled) {
    return State::kRunningThrottled;
  } else if (priority == NicePriority::kRunning) {
    return State::kRunningNormal;
  } else if (priority == NicePriority::kPrioritized) {
    return State::kRunningPrioritized;
  }

  CHECK(false) << "priority: " << int(priority);
}

/*static*/
Task::State Task::queued2running(Task::State state) {
  if (state == State::kQueuedPrioritized) {
    return State::kRunningPrioritized;
  } else if (state == State::kQueuedThrottled) {
    return State::kRunningThrottled;
  } else if (state == State::kQueuedNormal) {
    return State::kRunningNormal;
  }

  CHECK(false) << "state: " << int(state);
}

void Task::run() {
  {
    std::lock_guard lock{mutex_};
    set_state(queued2running(state_), lock);
  }

  opts().func()();
}

void TaskQueue::push(std::shared_ptr<Task> task) {
  printf("> push(task[%d])], %d\n", task->state(), nice_priority());
  if (nice_priority() != NicePriority::kNone) {
    task->set_state(Task::nice2queued(nice_priority()));
  }

  {
    std::unique_lock lock{shared_mutex_};
    queue_.push_back(std::move(task));
  }

  sem_.release();
}

std::shared_ptr<Task> TaskQueue::pop() {
  if (!sem_.try_acquire()) {
    return nullptr;
  }
  return pop_impl();
}

std::shared_ptr<Task> TaskQueue::pop_blocking() {
  sem_.acquire();
  return pop_impl();
}

void TaskQueue::reap_finished() {
  std::unique_lock lock{shared_mutex_};
  reap_finished(lock);
}

void TaskQueue::reap_finished(const std::unique_lock<std::shared_mutex>&) {
  while (!queue_.empty() && queue_.front()->state() == Task::State::kFinished) {
    queue_.pop_front();
  }
}

void TaskQueue::unblock_workers(size_t n) { sem_.release(n); }

bool TaskQueue::is_empty() const {
  std::lock_guard lock{shared_mutex_};
  return queue_.empty();
}

std::shared_ptr<Task> TaskQueue::pop_impl() {
  std::unique_lock lock{shared_mutex_};
  reap_finished(lock);

  if (!queue_.empty()) {
    auto task_ptr = std::move(queue_.front());
    queue_.pop_front();
    return task_ptr;
  }

  return nullptr;
}

TaskQueue* TaskQueues::queue(NicePriority priority) {
  switch (priority) {
    case NicePriority::kThrottled:
      return &throttled_queue_;
    case NicePriority::kRunning:
      return &running_queue_;
    default:
      return &prioritized_queue_;
  }
}

void TaskQueues::push(std::shared_ptr<Task> task) {
  switch (task->nice_priority()) {
    case NicePriority::kThrottled:
      task->set_state(Task::State::kQueuedThrottled);
      break;
    case NicePriority::kRunning:
      task->set_state(Task::State::kQueuedNormal);
      break;
    default:
      task->set_state(Task::State::kQueuedPrioritized);
      break;
  }

  queue(task->nice_priority())->push(std::move(task));
}

}  // namespace theta
