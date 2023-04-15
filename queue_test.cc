#include "queue.h"

#include <glog/logging.h>

#include <array>

#include "gtest/gtest.h"

namespace theta {

TEST(Queue, ctor) {
  int v{5};
  Queue<int>::Link foo{&v};
  EXPECT_EQ(foo.get(), &v);
}

TEST(Queue, push_back_pop_front) {
  Queue<int> queue;

  std::array<int, 10> arr;
  for (size_t i = 0; i < arr.size(); i++) {
    arr[i] = 100 + i;
    queue.push_front(&arr[i]);
  }

  int expected = 100;
  while (true) {
    int* v = queue.pop_back();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
  }
  EXPECT_EQ(expected, 110);
}

TEST(Queue, push_front_pop_back) {
  Queue<int> queue;

  std::array<int, 10> arr;
  for (size_t i = 0; i < arr.size(); i++) {
    arr[i] = 100 + i;
    queue.push_back(&arr[i]);
  }

  int expected = 100;
  while (true) {
    int* v = queue.pop_front();
    if (!v) {
      break;
    }
    EXPECT_EQ(*v, expected++);
  }
  EXPECT_EQ(expected, 110);
}

}  // namespace theta
