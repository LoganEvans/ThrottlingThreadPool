#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "gtest/gtest.h"
#include "epoch_ptr.h"

namespace theta {
  TEST(EpochPtr, ctor) {
    EpochPtr ep;
  }
}
