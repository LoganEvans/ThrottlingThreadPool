#pragma once

#include "executor.h"

namespace theta {

class WorkQueue {
 public:
  using Func = ExecutorImpl::Func;

  void push(Func func);
  Func pop();

 private:
  
};

class Worker {
  enum class RunState {
    kThrottled,
    kRunning,
    kPrioritized,
  };


}

class Workers {
 public:
  static Workers& getInstance() {
    static Workers instance;
    return instance;
  }

  Workers(const Workers&) = delete;
  void operator=(const Workers&) = delete;

 private:
  Workers() {}
}

}  // namespace theta
