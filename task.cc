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
    } else if (state == State::kPrepping) {
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kQueuedThreadpool) {
    if (state == State::kPrepping) {
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kPrepping) {
    if (state == State::kRunning) {
      stats->waiting_delta(-1);
      stats->running_delta(1);
      worker_->set_nice_priority(opts().nice_priority());
    } else if (state == State::kThrottled) {
      stats->waiting_delta(-1);
      stats->throttled_delta(1);
      worker_->set_nice_priority(NicePriority::kThrottled);
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
      worker_->set_nice_priority(NicePriority::kThrottled);
    } else if (state == State::kFinished) {
      stats->running_delta(-1);
      stats->finished_delta(1);
      state_ = State::kFinished;
      worker_->set_nice_priority(NicePriority::kNormal);
      worker_ = nullptr;
      return state_;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kThrottled) {
    if (state == State::kRunning) {
      stats->throttled_delta(-1);
      stats->running_delta(1);
      worker_->set_nice_priority(opts().nice_priority());
    } else if (state == State::kFinished) {
      stats->throttled_delta(-1);
      stats->finished_delta(1);
      state_ = State::kFinished;
      worker_->set_nice_priority(NicePriority::kNormal);
      worker_ = nullptr;
      return state_;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kFinished) {
    return State::kFinished;
  } else {
    CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                 << " to " << static_cast<int>(state);
  }

  state_ = state;
  return state;
}

void Task::run(EpochPtr<Task> task) {
  DCHECK(this == task.operator->());

  auto* executor = opts().executor();
  if (executor->stats()->running_num_is_at_limit()) {
    set_state(State::kThrottled);
    executor->running_.push_back(std::move(task));
  } else {
    set_state(State::kRunning);
    executor->throttled_.push_back(std::move(task));
  }

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

ThrottleList::ThrottleList()
    : head_(new Task{Task::Opts{}}),
      tail_(new Task{Task::Opts{}}),
      throttle_head_(tail_) {
  std::scoped_lock lock{head_->mutex_, tail_->mutex_};
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

void ThrottleList::append(std::shared_ptr<Task> task) {
  while (true) {
    std::shared_ptr<Task> prev{nullptr};

    {
      std::lock_guard l{tail_->mutex_};
      prev = tail_->prev_;
    }

    std::lock(prev->mutex_, task->mutex_, tail_->mutex_);

    if (tail_->prev_ != prev) {
      continue;
    }

    task->prev_ = prev;
    task->next_ = tail_;

    prev->next_ = task;
    tail_->prev_ = task;

    std::unique_lock prev_lock(prev->mutex_, std::adopt_lock);
    std::unique_lock tail_lock(task->mutex_, std::adopt_lock);
    prev_lock.unlock();
    tail_lock.unlock();

    std::lock_guard<std::mutex> lock{task->mutex_, std::adopt_lock};
    if (std::atomic_load_explicit(&throttle_head_,
                                  std::memory_order::acquire) == tail_) {
      task->set_state(Task::State::kRunning, lock);
    } else {
      task->set_state(Task::State::kThrottled, lock);
    }
  }
}

void ThrottleList::remove(std::shared_ptr<Task> task) {
  while (true) {
    std::shared_ptr<Task> prev{nullptr};
    std::shared_ptr<Task> next{nullptr};

    {
      std::lock_guard l{task->mutex_};
      prev = task->prev_;
      next = task->next_;
    }

    std::scoped_lock lock{prev->mutex_, task->mutex_, next->mutex_};
    if (task->prev_ != prev || task->next_ != next) {
      continue;
    }

    if (std::atomic_load_explicit(&throttle_head_,
                                  std::memory_order::acquire) == task) {
      std::atomic_store_explicit(&throttle_head_, next,
                                 std::memory_order::release);
    }

    prev->next_ = std::move(next);
    next->prev_ = std::move(prev);
  }
}

void ThrottleList::throttle_one() {
  while (true) {
    std::shared_ptr<Task> prev{nullptr};
    std::shared_ptr<Task> thead =
        std::atomic_load_explicit(&throttle_head_, std::memory_order::acquire);

    if (thead == head_) {
      return;
    }

    {
      std::lock_guard l{thead->mutex_};

      if (std::atomic_load_explicit(&throttle_head_,
                                    std::memory_order::acquire) != thead) {
        continue;
      }

      prev = thead->prev_;
    }

    std::lock(thead->mutex_, prev->mutex_);
    if (std::atomic_load_explicit(&throttle_head_,
                                  std::memory_order::acquire) != thead ||
        thead->prev_ != prev) {
      continue;
    }

    std::atomic_store_explicit(&throttle_head_, prev,
                               std::memory_order::release);
    std::unique_lock lock{thead->mutex_, std::adopt_lock};
    lock.unlock();

    std::lock_guard<std::mutex> prev_lock{prev->mutex_, std::adopt_lock};
    prev->set_state(Task::State::kRunning, prev_lock);
  }
}

void ThrottleList::unthrottle_one() {
  while (true) {
    std::shared_ptr<Task> next{nullptr};
    std::shared_ptr<Task> thead =
        std::atomic_load_explicit(&throttle_head_, std::memory_order::acquire);

    if (thead == tail_) {
      return;
    }

    {
      std::lock_guard l{thead->mutex_};

      if (std::atomic_load_explicit(&throttle_head_,
                                    std::memory_order::acquire) != thead) {
        continue;
      }

      next = thead->next_;
    }

    std::lock(thead->mutex_, next->mutex_);
    if (std::atomic_load_explicit(&throttle_head_,
                                  std::memory_order::acquire) != thead ||
        thead->next_ != next) {
      continue;
    }

    std::atomic_store_explicit(&throttle_head_, next,
                               std::memory_order::release);
    std::unique_lock next_lock{next->mutex_, std::adopt_lock};
    next_lock.unlock();

    std::lock_guard<std::mutex> lock{thead->mutex_, std::adopt_lock};
    thead->set_state(Task::State::kRunning, lock);
  }
}

}  // namespace theta
