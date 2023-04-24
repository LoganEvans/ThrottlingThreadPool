#include "queue.h"

#include <glog/logging.h>

#include <array>

#include "gtest/gtest.h"

namespace theta {

TEST(Queue, push_back_pop_front) {
  static constexpr int kSize = 10;
  Queue<int> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    queue.push_back(new int{100 + i});
  }

  int expected = 100;
  int expected_size = kSize;
  while (true) {
    EXPECT_EQ(queue.size(), expected_size--);
    auto v = queue.pop_front();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
    delete v;
  }
  EXPECT_EQ(expected, 110);
}

TEST(Queue, push_front_pop_back) {
  static constexpr int kSize = 10;
  Queue<int> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    queue.push_front(new int{100 + i});
  }

  int expected = 100;
  int expected_size = kSize;
  while (true) {
    EXPECT_EQ(queue.size(), expected_size--);
    auto v = queue.pop_back();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
    delete v;
  }
  EXPECT_EQ(expected, 110);
}

}  // namespace theta
