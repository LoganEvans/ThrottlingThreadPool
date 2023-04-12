#include "queue.h"

#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "gtest/gtest.h"

namespace theta {

TEST(Queue, ctor) {
  int v{5};
  Queue<int>::Link foo{&v};
  EXPECT_EQ(foo.get(), &v);
}

TEST(Queue, push_back_pop_front) {
  Queue<int>::Link head;
  Queue<int>::Link tail;
  head.push_head(&tail);

  for (int i = 0; i < 10; i++) {
    tail.push_tail(new Queue<int>::Link{new int{i}});
  }

  for (int i = 9; i >= 0; i--) {
    auto* link = head.pop_head();
    EXPECT_EQ(*link->get(), i);
    delete link->get();
    delete link;
  }
}

}  // namespace theta
