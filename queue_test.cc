#include "queue.h"

#include <glog/logging.h>

#include <array>

#include "gtest/gtest.h"

namespace theta {

TEST(Queue, push_back_pop_front) {
  static constexpr int kSize = 10;
  Queue<int*> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    EXPECT_TRUE(queue.push_back(new int{100 + i}));
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

TEST(Queue, next_pow_2) {
  int v = 4;
  while (v < (1 << 30)) {
    EXPECT_EQ(Queue<int>::next_pow_2(v - 1), v);
    EXPECT_EQ(Queue<int>::next_pow_2(v), v);
    EXPECT_EQ(Queue<int>::next_pow_2(v + 1), v << 1);
    v <<= 1;
  }
}

TEST(Queue, push_back_full) {
  static constexpr int kSize = 16;
  Queue<int*> queue{QueueOpts{}.set_max_size(kSize)};

  for (int j = 0; j < 32; j++) {
    EXPECT_EQ(queue.next_pow_2(16), 16);
    EXPECT_EQ(queue.capacity(), kSize - 1);

    for (int i = 0; i < static_cast<int>(queue.capacity()); i++) {
      EXPECT_EQ(queue.size(), i);
      EXPECT_TRUE(queue.push_back(new int{100 + i}));
    }

    EXPECT_EQ(queue.size(), queue.capacity());
    EXPECT_EQ(queue.size(), kSize - 1);
    auto* v = new int{1000};
    EXPECT_FALSE(queue.push_back(v));
    delete v;

    int expected = 100;
    int expected_size = queue.capacity();
    while (true) {
      EXPECT_EQ(queue.size(), expected_size--);
      auto v = queue.pop_front();
      if (!v) {
        break;
      }
      EXPECT_EQ(*v, expected++);
      delete v;
    }

    EXPECT_EQ(queue.size(), 0);

    // Do everything again, but with a new offset
    v = new int{1000};
    EXPECT_TRUE(queue.push_back(v));
    delete queue.pop_front();
    EXPECT_EQ(queue.pop_front(), nullptr);
  }
}

TEST(Queue, flusher) {
  static constexpr int kSize = 10;
  Queue<int*> queue{QueueOpts{}};

  for (int i = 0; i < kSize; i++) {
    EXPECT_EQ(queue.size(), i);
    EXPECT_TRUE(queue.push_back(new int{100 + i}));
  }

  int expected = 100;
  for (auto* v : queue.flusher()) {
    EXPECT_EQ(*v, expected++);
    delete v;
  }
  EXPECT_EQ(expected, 110);
}

}  // namespace theta
