#include "task.h"

#include "executor.h"

namespace theta {

/*static*/
void Task::run(std::unique_ptr<Task> task) {
  ExecutorImpl* executor = task->opts().executor();
  task->set_state(Task::State::kRunning);
  executor->throttle_list_.append(task.get());

  getrusage(RUSAGE_THREAD, &task->begin_ru_);
  ExecutorImpl::get_tv(&task->begin_tv_);

  task->opts().func()();

  getrusage(RUSAGE_THREAD, &task->end_ru_);
  ExecutorImpl::get_tv(&task->end_tv_);

  executor->stats()->update_ema(&task->begin_ru_, &task->begin_tv_,
                                &task->end_ru_, &task->end_tv_);

  executor->unreserve_active();
  executor->throttle_list_.remove(task.release());
}

Task::Task(Opts opts)
    : opts_(opts),
      state_(State::kCreated),
      worker_(nullptr),
      throttle_list_(nullptr),
      prev_(nullptr),
      next_(nullptr) {
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

    if (state == State::kThrottled) {
      stats->running_delta(-1);
      stats->throttled_delta(1);
      worker()->set_nice_priority(NicePriority::kThrottled);
    } else if (state == State::kFinished) {
      stats->running_delta(-1);
      stats->finished_delta(1);
      worker()->set_nice_priority(NicePriority::kNormal);
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
      worker()->set_nice_priority(NicePriority::kNormal);
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
    flush_modifications(/*wait_for_mtx=*/true);
  }

  if (num_items > modification_queue_.capacity() / 2) {
    flush_modifications();
  }
}

void ThrottleList::remove(Task* task) {
  Modification mod{Modification::Op::kRemove, task};
  size_t num_items;
  while (!modification_queue_.push_back(mod, &num_items)) {
    flush_modifications(/*wait_for_mtx=*/true);
  }

  if (num_items > modification_queue_.capacity() / 2) {
    flush_modifications();
  }
}

uint32_t ThrottleList::running_limit(std::memory_order mem_order) const {
  Count count{count_, mem_order};
  return count.running_limit();
}

void ThrottleList::set_running_limit(size_t limit) {
  Count count{count_, std::memory_order::acquire};
  if (count.running_limit() == limit) {
    return;
  }
  count_.set_running_limit(limit, std::memory_order::release);

  flush_modifications();
}

void ThrottleList::flush_modifications(bool wait_for_mtx) {
  // TODO(lpe): Setting the throttle state (Worker::set_nice_priority) takes
  // quite a while and should be moved outside of the lock. Similarly,
  // deallocations should be moved outside of the lock.

  std::unique_lock<std::mutex> lock{mtx_, std::defer_lock};
  if (wait_for_mtx) {
    lock.lock();
  } else if (!lock.try_lock()) {
    return;
  }

  uint32_t running_limit = count_.running_limit(std::memory_order::relaxed);
  while (true) {
    CHECK(verify_throttle()) << throttle_list_to_string();
    Modification mod = modification_queue_.pop_front();
    if (!mod) {
      break;
    }

    auto total = count_.total_delta(1);

    Task* task{mod.task()};
    switch (mod.op()) {
      case Modification::Op::kAppend:
        total = count_.total_delta(1);

        task->prev_ = tail_->prev_;
        task->next_ = tail_;
        tail_->prev_->next_ = task;
        tail_->prev_ = task;

        if (throttle_head_ != tail_) {
          task->set_state(Task::State::kThrottled);
        } else if (total > running_limit) {
          throttle_head_ = task;
          task->set_state(Task::State::kThrottled);
        } else {
          task->set_state(Task::State::kRunning);
          count_.running_delta(1);
        }

        DCHECK(task->next_);
        DCHECK(task->prev_);
        CHECK(verify_throttle()) << throttle_list_to_string();
        break;
      default:  // Modification::Op::kRemove:
        auto before = throttle_list_to_string();
        total = count_.total_delta(-1);

        task->next_->prev_ = task->prev_;
        task->prev_->next_ = task->next_;

        auto state = task->state();
        if (state == Task::State::kRunning) {
          DCHECK(throttle_head_ != task);

          if (throttle_head_ != tail_) {
            throttle_head_->set_state(Task::State::kRunning);
            throttle_head_ = throttle_head_->next_;
          } else {
            count_.running_delta(-1);
          }
        } else if (task == throttle_head_) {
          throttle_head_ = task->next_;
        }

        task->set_state(Task::State::kFinished);

        delete task;

        CHECK(verify_throttle())
            << before << "\n\n"
            << "removed: " << std::to_string(reinterpret_cast<size_t>(task))
            << "\n\n"
            << throttle_list_to_string();
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
    CHECK(throttle_head_ != head_);
    CHECK(throttle_head_->state() != Task::State::kThrottled);
    throttle_head_->set_state(Task::State::kThrottled);
    running--;
  }
}

bool ThrottleList::verify_throttle() const {
  if (head_ == throttle_head_) {
    return false;
  }

  if (head_->state() != Task::State::kCreated) {
    return false;
  }

  if (tail_->state() != Task::State::kCreated) {
    return false;
  }

  Task* curs = head_->next_;
  bool is_throttled_half = false;
  while (curs != tail_) {
    if (curs == throttle_head_) {
      is_throttled_half = true;
    }

    if (is_throttled_half && curs->state() != Task::State::kThrottled) {
      fprintf(stderr, "is_throttled_half... state: %d\n", curs->state());
      return false;
    } else if (!is_throttled_half && curs->state() != Task::State::kRunning) {
      fprintf(stderr, "!is_throttled_half... state: %d\n", curs->state());
      return false;
    }

    curs = curs->next_;
  }

  return true;
}

std::string ThrottleList::throttle_list_to_string() const {
  std::string s{"\n"};
  s += std::to_string(reinterpret_cast<size_t>(head_));
  s += ", state: " + std::to_string(int(head_->state()));
  s += " (head_)\n";

  Task* curs = head_->next_;

  while (curs != tail_) {
    s += std::to_string(reinterpret_cast<size_t>(curs));
    s += ", state: " + std::to_string(int(curs->state()));

    if (curs == throttle_head_) {
      s += " (throttle_head_)";
    }
    s += "\n";
    curs = curs->next_;
  }

  s += std::to_string(reinterpret_cast<size_t>(tail_));
  s += ", state: " + std::to_string(int(tail_->state()));
  s += " (tail_)";

  if (tail_ == throttle_head_) {
    s += " (throttle_head_)";
  }
  s += "\n";
  return s;
}

}  // namespace theta
