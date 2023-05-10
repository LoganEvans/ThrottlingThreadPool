#include "fifo_executor.h"

#include <glog/logging.h>

#include <optional>

namespace theta {

FIFOExecutorImpl::~FIFOExecutorImpl() {}

void FIFOExecutorImpl::post(Executor::Func func) {
  auto* task = new Task{Task::Opts{}.set_func(func).set_executor(this)};

  task->set_state(Task::State::kQueuedExecutor);

  if (!fast_post_queue_.push_back(task)) {
    shuffle_fifo_queues(/*task_to_post=*/task, /*task_to_pop=*/nullptr);
  }

  refill_queues();
}

std::unique_ptr<Task> FIFOExecutorImpl::pop() {
  // TODO(lpe): Remove this debug code.
  static std::atomic<std::chrono::time_point<std::chrono::system_clock>>
      last_print = std::chrono::system_clock::now();
  static std::mutex mtx;
  static constexpr auto threshold = std::chrono::microseconds{100000};

  auto now = std::chrono::system_clock::now();
  if (now - last_print.load(std::memory_order::acquire) > threshold) {
    std::lock_guard l{mtx};
    if (now - last_print.load(std::memory_order::acquire) > threshold) {
      last_print.store(now, std::memory_order::release);
      fprintf(stderr, "%s\n", debug_string().c_str());
    }
  }
  // Remove to here.

  Task* task = fast_pop_queue_.pop_front();
  if (!task) {
    shuffle_fifo_queues(/*task_to_post=*/nullptr, /*task_to_pop=*/&task);
  }
  return std::unique_ptr<Task>{task};
}

void FIFOExecutorImpl::shuffle_fifo_queues(Task* task_to_post,
                                           Task** task_to_pop) {
  std::lock_guard l{mu_};

  bool use_fast_pop_queue = true;

  if (task_to_pop) {
    // There is probably nothing here, but if there is, it's first priority.
    *task_to_pop = fast_pop_queue_.pop_front();
    if (!*task_to_pop && !slow_queue_.empty()) {
      *task_to_pop = slow_queue_.front();
      slow_queue_.pop();
    }
  }

  while (!slow_queue_.empty()) {
    Task* task = slow_queue_.front();

    if (!fast_pop_queue_.push_back(task)) {
      use_fast_pop_queue = false;
      break;
    }

    slow_queue_.pop();
  }

  if (task_to_pop && !*task_to_pop) {
    *task_to_pop = fast_post_queue_.pop_front();
  }

  while (true) {
    Task* task = fast_post_queue_.pop_front();
    if (!task) {
      break;
    }

    if (use_fast_pop_queue) {
      if (!fast_pop_queue_.push_back(task)) {
        use_fast_pop_queue = false;
      }
    }

    if (!use_fast_pop_queue) {
      slow_queue_.push(task);
    }
  }

  if (task_to_post) {
    if (!use_fast_pop_queue || !fast_pop_queue_.push_back(task_to_post)) {
      slow_queue_.push(task_to_post);
    }
  }
}

}  // namespace theta
