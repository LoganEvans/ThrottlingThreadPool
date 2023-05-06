#include "task.h"

#include "executor.h"

namespace theta {

/*static*/
void Task::run(std::unique_ptr<Task> task) {
  ExecutorImpl* executor = task->opts().executor();
  task->set_state(State::kRunning);
  executor->throttle_list_.append(task.get());
  task->opts().func()();
  task->set_state(Task::State::kFinished);
  executor->throttle_list_.remove(task.release());
}

Task::State Task::state(std::memory_order mem_order) const {
  return state_.load(mem_order);
}

Task::State Task::set_state(State state) {
  State old = state_.load(std::memory_order::acquire);
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
    } else if (state == State::kRunning) {
      stats->waiting_delta(-1);
      stats->running_delta(1);
      worker()->set_nice_priority(opts().nice_priority());
    } else if (state == State::kThrottled) {
      stats->waiting_delta(-1);
      stats->throttled_delta(1);
      worker()->set_nice_priority(NicePriority::kThrottled);
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kQueuedThreadpool) {
    if (state == State::kRunning) {
      stats->waiting_delta(-1);
      stats->running_delta(1);
      worker()->set_nice_priority(opts().nice_priority());
    } else if (state == State::kThrottled) {
      stats->waiting_delta(-1);
      stats->throttled_delta(1);
      worker()->set_nice_priority(NicePriority::kThrottled);
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kRunning) {
    rusage end_ru;
    getrusage(RUSAGE_THREAD, &end_ru);
    timeval end_tv;
    ExecutorImpl::get_tv(&end_tv);
    stats->update_ema_usage_proportion(&begin_ru_, &begin_tv_, &end_ru,
                                       &end_tv);

    if (state == State::kThrottled) {
      stats->running_delta(-1);
      stats->throttled_delta(1);
      worker()->set_nice_priority(NicePriority::kThrottled);
    } else if (state == State::kFinished) {
      stats->running_delta(-1);
      stats->finished_delta(1);
      state_.store(State::kFinished, std::memory_order::release);
      worker()->set_nice_priority(NicePriority::kNormal);
      return State::kFinished;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kThrottled) {
    if (state == State::kRunning) {
      stats->throttled_delta(-1);
      stats->running_delta(1);
      worker()->set_nice_priority(opts().nice_priority());
    } else if (state == State::kFinished) {
      stats->throttled_delta(-1);
      stats->finished_delta(1);
      state_.store(State::kFinished, std::memory_order::release);
      worker()->set_nice_priority(NicePriority::kNormal);
      return State::kFinished;
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else if (old == State::kFinished) {
    if (state == State::kRemoved) {
      state_.store(State::kRemoved, std::memory_order::release);
    } else {
      CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                   << " to " << static_cast<int>(state);
    }
  } else {
    CHECK(false) << "Illegal transition from " << static_cast<int>(old)
                 << " to " << static_cast<int>(state);
  }

  state_.store(state, std::memory_order::release);
  return state;
}

Worker* Task::worker(std::memory_order mem_order) const {
  return worker_.load(mem_order);
}

void Task::set_worker(Worker* val, std::memory_order mem_order) {
  worker_.store(val, mem_order);
}

ThrottleList::ThrottleList(size_t modification_queue_size)
    : head_(&head_real_),
      tail_(&tail_real_),
      throttle_head_(tail_),
      modification_queue_(QueueOpts{}.set_max_size(modification_queue_size)) {
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

void ThrottleList::append(Task* task) {
  Modification mod{Modification::Op::kAppend, task};
  size_t num_items;
  while (!modification_queue_.push_back(mod, &num_items)) {
    flush_modifications();
  }
}

void ThrottleList::remove(Task* task) {
  Modification mod{Modification::Op::kRemove, task};
  size_t num_items;
  while (!modification_queue_.push_back(mod, &num_items)) {
    flush_modifications();
  }

  if (num_items > modification_queue_.capacity() / 2) {
    flush_modifications();
  }
}

void ThrottleList::set_running_limit(size_t running_limit) {
  Count count{count_, std::memory_order::acquire};
  if (count.running_limit() == running_limit) {
    return;
  }

  flush_modifications();
}

void ThrottleList::flush_modifications(bool wait_for_mtx) {
  std::unique_lock<std::mutex> lock{mtx_, std::defer_lock};
  if (wait_for_mtx) {
    lock.lock();
  } else if (!lock.try_lock()) {
    return;
  }

  while (true) {
    Modification mod = modification_queue_.pop_front();
    if (!mod) {
      break;
    }
    Task* task{mod.task()};
    switch (mod.op()) {
      case Modification::Op::kAppend:
        task->prev_ = tail_->prev_;
        task->next_ = tail_;
        tail_->prev_->next_ = task;
        tail_->prev_ = task;
        DCHECK(task->next_);
        DCHECK(task->prev_);
        break;
      default:  // Modification::Op::kRemove:
        DCHECK(task->state() == Task::State::kFinished);
        task->set_state(Task::State::kRemoved);
        if (task->next_ && task->prev_) {
          DCHECK(task->next_);
          DCHECK(task->prev_);
          task->next_->prev_ = task->prev_;
          task->prev_->next_ = task->next_;
          task->next_ = nullptr;
          task->prev_ = nullptr;
        } else {
          //CHECK(false);
          fprintf(stderr, "error -- task already removed\n");
        }

        if (throttle_head_ == task) {
          throttle_head_ = task->next_;
        }

        delete task;

        break;
    };
  }

  adjust_throttle_head(lock);
}

void ThrottleList::adjust_throttle_head(std::unique_lock<std::mutex>&) {
  Count count{count_, /*mem_order=*/std::memory_order::acquire};
  uint32_t running = count.running();
  uint32_t running_limit = count.running_limit();

  while (running < running_limit) {
    if (throttle_head_ == tail_) {
      break;
    }
    throttle_head_->set_state(Task::State::kRunning);
    throttle_head_ = throttle_head_->next_;
    running++;
  }

  while (running > running_limit) {
    if (throttle_head_->prev_ == head_) {
      break;
    }
    throttle_head_ = throttle_head_->prev_;
    throttle_head_->set_state(Task::State::kThrottled);
    running--;
  }
}

}  // namespace theta
