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

//TYPED_TEST(QueueTests, push_back_pop_front) {
//  static constexpr int kSize = 10;
//  auto queue = this->make_uut();
//
//  for (uint64_t i = 0; i < kSize; i++) {
//    EXPECT_EQ(queue.size(), i);
//    EXPECT_TRUE(queue.push_back(new uint64_t{100 + i}));
//  }
//
//  int expected = 100;
//  int expected_size = kSize;
//  while (true) {
//    EXPECT_EQ(queue.size(), expected_size--);
//    auto v = queue.pop_front();
//    if (!v) {
//      break;
//    }
//    EXPECT_EQ(*v, expected++);
//    delete v;
//  }
//  EXPECT_EQ(expected, 110);
//}
//
//TYPED_TEST(QueueTests, next_pow_2) {
//  auto queue = this->make_uut();
//  int v = 4;
//  while (v < (1 << 30)) {
//    EXPECT_EQ(queue.next_pow_2(v - 1), v);
//    EXPECT_EQ(queue.next_pow_2(v), v);
//    EXPECT_EQ(queue.next_pow_2(v + 1), v << 1);
//    v <<= 1;
//  }
//}
//
//TYPED_TEST(QueueTests, push_back_full) {
//  static constexpr int kSize = 16;
//  auto queue = this->make_uut(QueueOpts{}.set_max_size(kSize));
//
//  for (int j = 0; j < 32; j++) {
//    EXPECT_EQ(queue.next_pow_2(16), 16);
//    EXPECT_EQ(queue.capacity(), kSize - 1);
//
//    for (uint64_t i = 0; i < queue.capacity(); i++) {
//      EXPECT_EQ(queue.size(), i);
//      EXPECT_TRUE(queue.push_back(new uint64_t{100 + i}));
//    }
//
//    EXPECT_EQ(queue.size(), queue.capacity());
//    EXPECT_EQ(queue.size(), kSize - 1);
//    auto* v = new uint64_t{1000};
//    EXPECT_FALSE(queue.push_back(v));
//    delete v;
//
//    int expected = 100;
//    int expected_size = queue.capacity();
//    while (true) {
//      EXPECT_EQ(queue.size(), expected_size--);
//      auto v = queue.pop_front();
//      if (!v) {
//        break;
//      }
//      EXPECT_EQ(*v, expected++);
//      delete v;
//    }
//
//    EXPECT_EQ(queue.size(), 0);
//
//    // Do everything again, but with a new offset
//    v = new uint64_t{1000};
//    EXPECT_TRUE(queue.push_back(v));
//    delete queue.pop_front();
//    EXPECT_EQ(queue.pop_front(), nullptr);
//  }
//}

TYPED_TEST(QueueTests, multithreaded_stress) {
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
              auto* v = queue.pop_front();
              if (v) {
                sum += *v;
                delete v;
              }
            } else {
              uint64_t* v = new uint64_t{num_pushes++};
              while (!queue.push_back(v)) {
                auto* other = queue.pop_front();
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
    auto v = queue.pop_front();
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
