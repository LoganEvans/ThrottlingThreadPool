#include "task.h"

#include <glog/logging.h>

#include "executor.h"

namespace theta {

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
  // TODO(lpe): Add an "wanted running state" because tasks can be in the
  // wrong queue (because a throttling/unthrottling event occured), but can't be
  // removed. That means it's possible for a task to be popped from the
  // throttled queue even though it will need to run with normal priority.
  // That sholud obviate the need for this function.
  if (state == State::kQueuedPrioritized) {
    return State::kRunningPrioritized;
  } else if (state == State::kQueuedThrottled) {
    return State::kRunningThrottled;
  } else if (state == State::kQueuedNormal) {
    return State::kRunningNormal;
  }

  CHECK(false) << "state: " << int(state);
}

/*static*/
bool Task::is_running_state(Task::State state) {
  if (state == State::kRunningPrioritized ||
      state == State::kRunningThrottled || state == State::kRunningNormal) {
    return true;
  }
  return false;
}

Task::State Task::state() const {
  std::lock_guard lock{mutex_};
  return state(lock);
}

Task::State Task::state(const std::lock_guard<std::mutex>&) const {
  return state_;
}

void Task::set_state(State state) {
  std::lock_guard lock{mutex_};
  set_state(state, lock);
}

void Task::set_state(State state, const std::lock_guard<std::mutex>&) {
  State old = state_;
  state_ = state;
  auto* executor = opts().executor();
  auto* stats = executor->stats();

  auto update_ema_usage_proportion = [&]() {
    rusage end_ru;
    getrusage(RUSAGE_THREAD, &end_ru);
    timeval end_tv;
    ExecutorImpl::get_tv(&end_tv);
    stats->update_ema_usage_proportion(&begin_ru_, &begin_tv_, &end_ru, &end_tv);
  };

  if (old == State::kCreated && state == State::kQueuedExecutor) {
    stats->waiting_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedPrioritized) {
    stats->running_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedThrottled) {
    stats->throttled_delta(1);
  } else if (old == State::kCreated && state == State::kQueuedNormal) {
    stats->running_delta(1);
  } else if (old == State::kQueuedExecutor &&
             state == State::kQueuedPrioritized) {
    stats->running_delta(1);
  } else if (old == State::kQueuedExecutor &&
             state == State::kQueuedThrottled) {
    stats->throttled_delta(1);
  } else if (old == State::kQueuedExecutor && state == State::kQueuedNormal) {
    stats->running_delta(1);
  } else if (old == State::kQueuedPrioritized &&
             state == State::kRunningPrioritized) {
  } else if (old == State::kQueuedPrioritized &&
             state == State::kQueuedThrottled) {
    stats->running_delta(-1);
    stats->throttled_delta(1);
  } else if (old == State::kQueuedThrottled &&
             state == State::kQueuedPrioritized) {
    stats->throttled_delta(-1);
    stats->running_delta(1);
  } else if (old == State::kQueuedThrottled &&
             state == State::kRunningThrottled) {
  } else if (old == State::kQueuedThrottled && state == State::kQueuedNormal) {
    stats->throttled_delta(-1);
    stats->running_delta(1);
  } else if (old == State::kQueuedNormal && state == State::kQueuedThrottled) {
    stats->running_delta(-1);
    stats->throttled_delta(1);
  } else if (old == State::kQueuedNormal && state == State::kRunningNormal) {
    getrusage(RUSAGE_THREAD, &begin_ru_);
    ExecutorImpl::get_tv(&begin_tv_);
  } else if (old == State::kRunningPrioritized &&
             state == State::kRunningThrottled) {
    getrusage(RUSAGE_THREAD, &begin_ru_);
    ExecutorImpl::get_tv(&begin_tv_);
    stats->running_delta(-1);
    stats->throttled_delta(1);
  } else if (old == State::kRunningPrioritized && state == State::kFinished) {
    update_ema_usage_proportion();
    stats->running_delta(-1);
    stats->finished_delta(1);
  } else if (old == State::kRunningThrottled &&
             state == State::kRunningPrioritized) {
    getrusage(RUSAGE_THREAD, &begin_ru_);
    ExecutorImpl::get_tv(&begin_tv_);
  } else if (old == State::kRunningThrottled &&
             state == State::kRunningNormal) {
    getrusage(RUSAGE_THREAD, &begin_ru_);
    ExecutorImpl::get_tv(&begin_tv_);
  } else if (old == State::kRunningThrottled && state == State::kFinished) {
    stats->throttled_delta(-1);
    stats->finished_delta(1);
  } else if (old == State::kRunningNormal && state == State::kRunningThrottled) {
  } else if (old == State::kRunningNormal && state == State::kFinished) {
    update_ema_usage_proportion();
    stats->running_delta(-1);
    stats->finished_delta(1);
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

void Task::run() {
  if (is_running_state(state())) {
    // TODO(lpe): This happens because of how the queues are implemented. A task
    // can end up in both the normal and the throttled queue at the same time
    // because there's currently no way to remove a task from the middle of a
    // queue.
    return;
  }

  {
    std::lock_guard lock{mutex_};
    set_state(queued2running(state_), lock);
  }

  opts().func()();
  set_state(State::kFinished);
}

void TaskQueue::push(std::shared_ptr<Task> task) {
  DCHECK(task);

  if (nice_priority() != NicePriority::kNone) {
    task->set_state(Task::nice2queued(nice_priority()));
  }

  {
    std::unique_lock lock{shared_mutex_};
    queue_.push_back(std::move(task));
  }

  sem_.release();
}

void TaskQueue::push_front(std::shared_ptr<Task> task) {
  DCHECK(task);

  if (nice_priority() != NicePriority::kNone) {
    task->set_state(Task::nice2queued(nice_priority()));
  }

  {
    std::unique_lock lock{shared_mutex_};
    queue_.push_front(std::move(task));
  }

  sem_.release();
}

std::shared_ptr<Task> TaskQueue::maybe_pop() {
  if (!sem_.try_acquire()) {
    return nullptr;
  }
  return pop_impl();
}

std::shared_ptr<Task> TaskQueue::wait_pop() {
  sem_.acquire();
  return pop_impl();
}

std::shared_ptr<Task> TaskQueue::maybe_pop_back() {
  if (!sem_.try_acquire()) {
    return nullptr;
  }

  std::unique_lock lock{shared_mutex_};
  reap_finished_back(lock);

  if (!queue_.empty()) {
    auto task = std::move(queue_.back());
    queue_.pop_back();
    return task;
  }

  return nullptr;
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

void TaskQueue::reap_finished_back(const std::unique_lock<std::shared_mutex>&) {
  while (!queue_.empty() && queue_.back()->state() == Task::State::kFinished) {
    queue_.pop_back();
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
    auto task = std::move(queue_.front());
    queue_.pop_front();
    return task;
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
