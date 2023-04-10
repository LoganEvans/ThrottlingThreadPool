#pragma once

#include <functional>

#include "HyperSharedPointer.h"

template <typename T>
class EpochPtr {
 public:
  T* get() const;
  void new_epoch();
  void defer(std::function<void()> func);

 private:
};

