#include "threadpool.h"

#include <array>
#include <thread>

using namespace std::chrono_literals;

namespace theta {

/*static*/
ThrottlingThreadpool::ConfigureOpts
ThrottlingThreadpool::ConfigureOpts::defaultOpts() {
  return ConfigureOpts{}
      .set_nice_cores(std::thread::hardware_concurrency() / 8)
      .set_thread_limit(1)//8 * std::thread::hardware_concurrency())
      .set_throttle_interval(100ms);
}

/*static*/
ThrottlingThreadpool& ThrottlingThreadpool::getInstance() {
  static ThrottlingThreadpool instance;
  return instance;
}

void ThrottlingThreadpool::configure(
    const ThrottlingThreadpool::ConfigureOpts& opts) {
  shared_mutex_.lock();
  opts_ = opts;
  shared_mutex_.unlock();
}

Executor ThrottlingThreadpool::create(Executor::Opts opts) {
  opts.set_run_queue(&run_queue_);
  std::unique_ptr<ExecutorImpl> impl;
  if (opts.priority_policy() == PriorityPolicy::FIFO) {
    impl = std::unique_ptr<FIFOExecutorImpl>(
        new FIFOExecutorImpl(std::move(opts)));
  } else {
    throw NotImplemented();
  }

  auto* ptr = impl.get();

  shared_mutex_.lock();
  executors_.push_back(std::move(impl));
  shared_mutex_.unlock();

  Executor ex{ptr};
  return ex;
}

ThrottlingThreadpool::~ThrottlingThreadpool() {
  for (auto& worker : workers_) {
    worker->shutdown();
  }

  run_queue_.shutdown();
}

ThrottlingThreadpool::ThrottlingThreadpool() {
  opts_ = ConfigureOpts::defaultOpts();

  workers_.reserve(opts_.thread_limit());
  for (size_t i = 0; i < opts_.thread_limit(); i++) {
    workers_.push_back(std::make_unique<Worker>(&run_queue_));
  }
}
}  // namespace theta
