#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <latch>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"
#include "threadpool.h"

namespace theta {

using namespace std::chrono_literals;

TEST(FIFOExecutor, DISABLED_ctor) {
  Executor executor = ThrottlingThreadpool::getInstance().create(
      Executor::Opts{}
          .set_priority_policy(PriorityPolicy::FIFO)
          .set_thread_weight(5)
          .set_worker_limit(2));

  EXPECT_EQ(executor.opts().priority_policy(), PriorityPolicy::FIFO);
  EXPECT_EQ(executor.opts().thread_weight(), 5);
  EXPECT_EQ(executor.opts().worker_limit(), 2);
}

TEST(FIFOExecutor, DISABLED_post) {
  std::condition_variable cv;
  std::mutex mu;

  Executor executor =
      ThrottlingThreadpool::getInstance().create(Executor::Opts{});

  std::unique_lock<std::mutex> lock{mu};
  auto now = Executor::Clock::now();
  std::atomic<bool> jobRan{false};

  executor.post([&]() {
    jobRan.store(true, std::memory_order_release);
    cv.notify_all();
  });

  cv.wait_until(lock, now + 1s,
                [&]() { return jobRan.load(std::memory_order_acquire); });

  EXPECT_TRUE(jobRan.load(std::memory_order_acquire));
}

TEST(FIFOExecutor, DISABLED_saturate_single_thread) {
  static constexpr int kJobs = 1000000;

  Executor executor = ThrottlingThreadpool::getInstance().create(
      Executor::Opts{}
          .set_priority_policy(PriorityPolicy::FIFO)
          .set_thread_weight(1)
          .set_worker_limit(1));

  std::latch work_start{kJobs};
  std::latch work_done{kJobs};
  std::atomic<int> jobsRun{0};

  auto job = std::function<void()>([&]() {
    work_start.wait();
    jobsRun.fetch_add(1, std::memory_order_acq_rel);
    work_done.count_down(1);
  });

  for (int i = 0; i < kJobs; i++) {
    executor.post(job);
  }
  work_start.count_down(kJobs);

  fprintf(stderr, "start\n");
  while (!work_done.try_wait()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(jobsRun.load(std::memory_order_acquire), kJobs);
}

TEST(FIFOExecutor, saturate_many_threads) {
  static constexpr int kJobs = 1000000;

  auto num_threads = std::thread::hardware_concurrency();
  Executor executor = ThrottlingThreadpool::getInstance().create(
      Executor::Opts{}
          .set_priority_policy(PriorityPolicy::FIFO)
          .set_thread_weight(num_threads)
          .set_worker_limit(Executor::Opts::kNoWorkerLimit));

  std::latch work_start{kJobs};
  std::latch work_done{kJobs};
  std::atomic<int> jobsRun{0};

  auto job = std::function<void()>([&]() {
    work_start.wait();
    jobsRun.fetch_add(1, std::memory_order_acq_rel);
    work_done.count_down(1);
  });

  for (int i = 0; i < kJobs; i++) {
    executor.post(job);
  }
  work_start.count_down(kJobs);

  fprintf(stderr, "start\n");
  while (!work_done.try_wait()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(jobsRun.load(std::memory_order_acquire), kJobs);
}

}  // namespace theta
