#include "task.h"

#include "executor.h"

namespace theta {

/*static*/
void Task::run(ExecutorImpl* executor, std::unique_ptr<Task> task) {
  std::unique_lock lock{executor->mu_, std::defer_lock};
  executor->throttle_list_.append(task.get(), lock);
  task->opts().func()();
  executor->throttle_list_.remove(task.release(), lock);
}

Task::State Task::state() const { return state_; }

Task::State Task::set_state(State state) {
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

ThrottleList::ThrottleList(size_t modification_queue_size)
    : head_(new Task{Task::Opts{}}),
      tail_(new Task{Task::Opts{}}),
      throttle_head_(tail_),
      modification_queue_(QueueOpts{}.set_max_size(modification_queue_size)) {
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

void ThrottleList::append(Task* task, std::unique_lock<std::mutex>& lock) {
  Modification mod{Modification::Op::kAppend, task};
  size_t num_items;
  while (!modification_queue_.push_back(mod, &num_items)) {
    flush_modifications(lock);
  }
}

void ThrottleList::remove(Task* task, std::unique_lock<std::mutex>& lock) {
  Modification mod{Modification::Op::kRemove, task};
  size_t num_items;
  while (!modification_queue_.push_back(mod, &num_items)) {
    flush_modifications(lock);
  }

  if (num_items >= modification_queue_.capacity() / 2 && lock.try_lock()) {
    flush_modifications(lock);
    lock.unlock();
  }
}

void ThrottleList::throttle(size_t n, std::unique_lock<std::mutex>& lock) {
  Modification mod{Modification::Op::kThrottle, n};
  modification_queue_.push_back(mod);
  flush_modifications(lock);
}

void ThrottleList::unthrottle(size_t n, std::unique_lock<std::mutex>& lock) {
  Modification mod{Modification::Op::kUnthrottle, n};
  modification_queue_.push_back(mod);
  flush_modifications(lock);
}

void ThrottleList::flush_modifications(std::unique_lock<std::mutex>& lock) {
  bool was_locked = true;
  if (!lock) {
    was_locked = false;
    lock.lock();
  }

  for (Modification mod : modification_queue_.flusher()) {
    Task* task{nullptr};
    switch (mod.op()) {
      case Modification::Op::kAppend:
        task = mod.task();
        task->prev_ = tail_->prev_;
        task->next_ = tail_;
        tail_->prev_->next_ = task;
        tail_->prev_ = task;
        if (throttle_head_ != tail_) {
          task->set_state(Task::State::kThrottled);
        } else {
          task->set_state(Task::State::kRunning);
        }
        break;
      case Modification::Op::kRemove:
        task = mod.task();
        task->next_->prev_ = task->prev_;
        task->prev_->next_ = task->next_;
        task->set_state(Task::State::kFinished);
        delete task;
        break;
      case Modification::Op::kThrottle:
        if (throttle_head_ == head_) {
          break;
        }
        for (size_t i = 0; i < mod.count(); i++) {
          if (!throttle_head_->prev_) {
            break;
          }
          throttle_head_ = throttle_head_->prev_;
          throttle_head_->set_state(Task::State::kThrottled);
        }
        break;
      default:
        for (size_t i = 0; i < mod.count(); i++) {
          if (throttle_head_ == tail_) {
            break;
          }
          throttle_head_->set_state(Task::State::kRunning);
          throttle_head_ = throttle_head_->next_;
        }
        break;
    };
  }

  if (was_locked && !lock) {
    lock.lock();
  } else if (!was_locked && lock) {
    lock.unlock();
  }
}

}  // namespace theta
