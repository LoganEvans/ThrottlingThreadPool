#include "threadpool.h"

#include <thread>

using namespace std::chrono_literals;

namespace theta {

/*static*/
ScalingThreadpool::ConfigureOpts
ScalingThreadpool::ConfigureOpts::defaultOpts() {
  return ConfigureOpts{}
      .set_nice_cores(std::thread::hardware_concurrency() / 8)
      .set_thread_limit(8 * std::thread::hardware_concurrency())
      .set_throttle_interval(100ms);
}

void ScalingThreadpool::configure(const ScalingThreadpool::ConfigureOpts& opts) {
  shared_mutex_.lock();
  opts_ = opts;
  shared_mutex_.unlock();
}

ScalingThreadpool::~ScalingThreadpool() {
  shutdown_.store(true, std::memory_order_release);
  normal_prio_sem_.release(threads_.size());

  for (auto& thread : threads_) {
    thread.join();
  }
}

ScalingThreadpool::ScalingThreadpool() {
  opts_ = ConfigureOpts::defaultOpts();

  // TODO(lpe): Throttled and limit CPUs!
  threads_.reserve(opts_.thread_limit());
  for (size_t i = 0; i < opts_.thread_limit(); i++) {
    threads_.push_back(std::thread{&ScalingThreadpool::task_loop, this});
  }
}

ExecutorImpl* ScalingThreadpool::create(const ExecutorImpl::Opts& opts) {
  shared_mutex_.lock();

  ExecutorImpl* ptr = nullptr;
  if (opts.priority_policy() == PriorityPolicy::FIFO) {
    auto uptr = std::unique_ptr<FIFOExecutorImpl>(new FIFOExecutorImpl(opts));
    ptr = uptr.get();
    executors_.push_back(std::move(uptr));
  } else {
    throw NotImplemented();
  }

  shared_mutex_.unlock();
  return ptr;
}

void ScalingThreadpool::task_loop() {
  while (true) {
    normal_prio_sem_.acquire();

    if (shutdown_.load(std::memory_order_relaxed)) {
      break;
    }

    shared_mutex_.lock_shared();
    Executor::Func func{nullptr};
    for (auto& executor : executors_) {
      func = executor->pop();
      if (func) {
        break;
      }
    }

    shared_mutex_.unlock_shared();

    if (func) {
      func();
    } else {
      normal_prio_sem_.release();
    }
  }
}

void ScalingThreadpool::notify_new_task() {
  normal_prio_sem_.release();
}

void Executor::post(Executor::Func func) {
  printf("> post()\n");
  impl_->post(func);
  ScalingThreadpool::getInstance().notify_new_task();
  printf("< post()\n");
}

Executor::Func Executor::pop() {
  return impl_->pop();
}

}
