#include "semaphore.h"

#include <glog/logging.h>

#include "gtest/gtest.h"

namespace theta {

TEST(Semaphore, release) {
  Semaphore s;

  EXPECT_EQ(s.count(), 0);

  s.release();
  EXPECT_EQ(s.count(), 1);

  s.release(100);
  EXPECT_EQ(s.count(), 101);
}

TEST(Semaphore, acquire) {
  Semaphore s;

  EXPECT_EQ(s.count(), 0);
  s.release();
  EXPECT_EQ(s.count(), 1);
  s.acquire();
  EXPECT_EQ(s.count(), 0);
}

TEST(Semaphore, try_acquire_false) {
  Semaphore s;

  EXPECT_FALSE(s.try_acquire());

  s.release();
  s.acquire();

  EXPECT_FALSE(s.try_acquire());
}

TEST(Semaphore, try_acquire_true) {
  Semaphore s;

  s.release();
  EXPECT_TRUE(s.try_acquire());
}

}
