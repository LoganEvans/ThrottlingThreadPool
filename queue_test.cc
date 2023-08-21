#include "queue.h"

#include <glog/logging.h>

#include <array>
#include <random>
#include <shared_mutex>

#include "gtest/gtest.h"

namespace theta {

template <typename T>
class QueueTests : public ::testing::Test {
 public:
  static T make_uut(QueueOpts opts = QueueOpts{}) {
    return T{opts};
  }
};

//using MyTypes = ::testing::Types<MPSCQueue<uint64_t*>, Queue<uint64_t*>>;
using MyTypes = ::testing::Types<Queue<uint64_t*>>;
TYPED_TEST_SUITE(QueueTests, MyTypes);

TYPED_TEST(QueueTests, push_pop) {
  static constexpr int kSize = 10;
  auto queue = this->make_uut();

  for (uint64_t i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    queue.push(new uint64_t{100 + i});
  }

  int expected = 100;
  int expected_size = kSize;
  while (queue.size()) {
    EXPECT_EQ(queue.size(), expected_size--);
    auto v = queue.pop();
    EXPECT_EQ(*v, expected++);
    delete v;
  }
  EXPECT_EQ(expected, 110);
}

TYPED_TEST(QueueTests, DISABLED_push_full) {
  static constexpr int kSize = 16;
  auto queue = this->make_uut(QueueOpts{}.set_max_size(kSize));

  for (int j = 0; j < 32; j++) {
    for (uint64_t i = 0; i < queue.capacity(); i++) {
      EXPECT_EQ(queue.size(), i);
      queue.push(new uint64_t{100 + i});
    }

    EXPECT_EQ(queue.size(), queue.capacity());
    unsigned long* v = new uint64_t{1000};
    queue.push(std::move(v));
    delete v;

    int expected = 100;
    int expected_size = queue.capacity();
    while (true) {
      EXPECT_EQ(queue.size(), expected_size--);
      auto v = queue.pop();
      if (!v) {
        break;
      }
      EXPECT_EQ(*v, expected++);
      delete v;
    }

    EXPECT_EQ(queue.size(), 0);

    // Do everything again, but with a new offset
    v = new uint64_t{1000};
    queue.push(std::move(v));
    delete queue.pop();
    EXPECT_EQ(queue.pop(), nullptr);
  }
}

TYPED_TEST(QueueTests, DISABLED_multithreaded_stress) {
  static constexpr uint64_t kPushesPerThread = 100000;
  static constexpr int kNumThreads = 4;
  std::array<std::thread, kNumThreads> threads;
  std::array<std::atomic<uint64_t>, kNumThreads> sums;

  auto queue = this->make_uut(QueueOpts{}.set_max_size(32));

  for (int tx = 0; tx < kNumThreads; tx++) {
    threads[tx] = std::thread(
        [&](int tx) {
          std::default_random_engine gen;
          gen.seed(tx);
          std::uniform_real_distribution<double> unif(0.0, 1.0);

          uint64_t sum = 0;
          uint64_t num_pushes = 0;

          while (num_pushes < kPushesPerThread) {
            double choice = unif(gen);
            if (choice < 0.5) {
              auto* v = queue.pop();
              if (v) {
                sum += *v;
                delete v;
              }
            } else {
              uint64_t* v = new uint64_t{num_pushes++};
              while (!queue.try_push(v)) {
                auto* other = queue.pop();
                if (other) {
                  sum += *other;
                  delete other;
                }
              }
            }
          }

          sums[tx].store(sum);
        },
        tx);
  }

  uint64_t total_sum = 0;
  for (int tx = 0; tx < kNumThreads; tx++) {
    threads[tx].join();
    total_sum += sums[tx];
  }

  while (true) {
    auto v = queue.pop();
    if (!v) {
      break;
    }
    total_sum += *v;
    delete v;
  }

  EXPECT_EQ(total_sum,
            kNumThreads * kPushesPerThread * (kPushesPerThread - 1) / 2);
}

}  // namespace theta
