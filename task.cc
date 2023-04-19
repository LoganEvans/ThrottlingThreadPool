#include "task.h"

#include <glog/logging.h>

#include "executor.h"

namespace theta {

Task::State Task::state() const {
  std::lock_guard lock{mutex_};
  return state(lock);
}

Task::State Task::state(const std::lock_guard<std::mutex>&) const {
  return state_;
}

Task::State Task::set_state(State state) {
  std::lock_guard lock{mutex_};
  return set_state(state, lock);
}

Task::State Task::set_state(State state, const std::lock_guard<std::mutex>&) {
  State old = state_;
  auto* executor = opts().executor();
  auto* stats = executor->stats();

  if (old == State::kCreated) {
    if (state == State::kQueuedExecutor) {
      stats->waiting_delta(1);
    } else if (state == State::kQueuedThreadpool) {
      stats->waiting_delta(1);
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kQueuedExecutor) {
    if (state == State::kQueuedThreadpool) {
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kQueuedThreadpool) {
    if (state == State::kRunning) {
      stats->waiting_delta(-1);
      stats->running_delta(1);
    } else if (state == State::kThrottled) {
      stats->waiting_delta(-1);
      stats->throttled_delta(1);
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kRunning) {
    rusage end_ru;
    getrusage(RUSAGE_THREAD, &end_ru);
    timeval end_tv;
    ExecutorImpl::get_tv(&end_tv);
    stats->update_ema_usage_proportion(&begin_ru_, &begin_tv_, &end_ru, &end_tv);

    if (state == State::kThrottled) {
      stats->running_delta(-1);
      stats->throttled_delta(1);
    } else if (state == State::kFinished) {
      stats->running_delta(-1);
      stats->finished_delta(1);
      state_ = State::kFinished;
      return state_;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kThrottled) {
    if (state == State::kRunning) {
      stats->throttled_delta(-1);
      stats->running_delta(1);
    } else if (state == State::kFinished) {
      stats->throttled_delta(-1);
      stats->finished_delta(1);
      state_ = State::kFinished;
      return state_;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  }

  state_ = state;
  return state;
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
  set_state(State::kRunning);
  opts().func()();
  set_state(State::kFinished);
}

void TaskQueue::shutdown() {
  shutdown_.store(true);
  sem_.release(std::numeric_limits<int32_t>::max() / 2);
}

bool TaskQueue::is_shutting_down() const {
  return shutdown_.load(std::memory_order_acquire);
}

void TaskQueue::push(EpochPtr<Task> task) {
  DCHECK(task);

  auto v = queue_.push_back(std::move(task));
  CHECK(!v.has_value()) << "queue_.size(): " << queue_.size();
  sem_.release();
}

void TaskQueue::push_front(EpochPtr<Task> task) {
  DCHECK(task);

  auto v = queue_.push_front(std::move(task));
  CHECK(!v.has_value());
  sem_.release();
}

std::optional<EpochPtr<Task>> TaskQueue::maybe_pop() {
  if (!sem_.try_acquire()) {
    return {};
  }

  auto v = queue_.pop_front();
  CHECK(v.has_value());
  return v;
}

EpochPtr<Task> TaskQueue::wait_pop() {
  while (true) {
    sem_.acquire();
    if (shutdown_.load(std::memory_order_acquire)) {
      return nullptr;
    }

    auto v = queue_.pop_front();
    if (v) {
      return std::move(v.value());
    }

    sem_.release(1);
  }
}

std::optional<EpochPtr<Task>> TaskQueue::maybe_pop_back() {
  if (!sem_.try_acquire()) {
    return {};
  }

  auto v = queue_.pop_back();
  CHECK(v.has_value());
  return v;
}

void TaskQueue::unblock_workers(size_t n) { sem_.release(n); }

}  // namespace theta
