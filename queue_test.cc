#include "queue.h"

#include <glog/logging.h>

#include <array>

#include "epoch.h"
#include "gtest/gtest.h"

namespace theta {

TEST(Queue, push_back_pop_front) {
  static constexpr int kSize = 10;
  Queue<int> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    queue.push_back(EpochPtr<int>::make(100 + i));
  }

  int expected = 100;
  int expected_size = kSize;
  while (true) {
    EXPECT_EQ(queue.size(), expected_size--);
    auto v = queue.pop_front();
    if (!v.has_value()) {
      break;
    }
    EXPECT_EQ(*v.value(), expected++);
  }
  EXPECT_EQ(expected, 110);
}

TEST(Queue, push_front_pop_back) {
  static constexpr int kSize = 10;
  Queue<int> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    queue.push_front(EpochPtr<int>::make(100 + i));
  }

  int expected = 100;
  int expected_size = kSize;
  while (true) {
    EXPECT_EQ(queue.size(), expected_size--);
    auto v = queue.pop_back();
    if (!v.has_value()) {
      break;
    }
    EXPECT_EQ(*v.value(), expected++);
  }
  EXPECT_EQ(expected, 110);
}

}  // namespace theta
