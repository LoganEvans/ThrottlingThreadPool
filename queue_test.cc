#include "queue.h"

#include <glog/logging.h>

#include <array>

#include "epoch.h"
#include "gtest/gtest.h"

namespace theta {

TEST(Queue, ctor) {
  EpochPtr<int> v{5};
  Queue<EpochPtr<int>>::Link foo{v, Epoch::get_allocator()};
  EXPECT_EQ(*foo.destroy(), *v);
}

TEST(Queue, push_back_pop_front) {
  Queue<EpochPtr<int>> queue;

  for (int i = 0; i < 10; i++) {
    queue.push_back(EpochPtr<int>{100 + i});
  }

  int expected = 100;
  while (true) {
    auto v = queue.pop_front();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
  }
  EXPECT_EQ(expected, 110);
}

TEST(Queue, push_front_pop_back) {
  Queue<EpochPtr<int>> queue;

  for (int i = 0; i < 10; i++) {
    queue.push_front(EpochPtr<int>{100 + i});
  }

  int expected = 100;
  while (true) {
    auto v = queue.pop_back();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
  }
  EXPECT_EQ(expected, 110);
}

}  // namespace theta
