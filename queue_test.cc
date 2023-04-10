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

}  // namespace theta
