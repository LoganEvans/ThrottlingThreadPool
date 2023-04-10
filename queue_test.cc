#include "queue.h"

#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "gtest/gtest.h"

namespace theta {

TEST(Queue, ctor) {
  Queue<int>::Link foo{4};
  EXPECT_EQ(foo.get(), 4);
}

}  // namespace theta
