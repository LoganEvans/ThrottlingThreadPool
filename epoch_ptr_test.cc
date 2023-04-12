#include "epoch_ptr.h"

#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

namespace theta {

TEST(MemoryPool, allocate) {
  MemoryPool mp;

  static constexpr int kNumVals{5000};
  std::vector<int*> vals;
  vals.reserve(kNumVals);

  for (int i = 0; i < kNumVals; i++) {
    vals.push_back(static_cast<int*>(mp.allocate(sizeof(int), alignof(int))));
    *vals.back() = i;
  }

  for (int i = 0; i < kNumVals; i++) {
    EXPECT_EQ(*vals[i], i);
  }
}

TEST(MemoryPool, allocateStruct) {
  struct Foo {
    int foo_in_epoch_0;
    int b;

    Foo(int aa, int bb) : foo_in_epoch_0(aa), b(bb) {}
  };

  MemoryPool mp;

  static constexpr int kNumVals{5000};
  std::vector<Foo*> vals;

  for (int i = 0; i < kNumVals; i++) {
    vals.push_back(mp.allocate<Foo>(i, 100 * i));
  }

  for (int i = 0; i < kNumVals; i++) {
    EXPECT_EQ(vals[i]->foo_in_epoch_0, i);
    EXPECT_EQ(vals[i]->b, 100 * i);
  }
}

TEST(CPULocalMemoryPools, allocate) {
  struct Foo {
    int foo_in_epoch_0;
    int b;

    Foo(int aa, int bb) : foo_in_epoch_0(aa), b(bb) {}
  };

  CPULocalMemoryPools mp;

  size_t num_cores = std::thread::hardware_concurrency();
  static constexpr int kNumVals{5000};

  std::vector<std::vector<Foo*>> vals;
  vals.resize(num_cores);

  for (size_t cpu = 0; cpu < std::thread::hardware_concurrency(); cpu++) {
    for (int i = 0; i < kNumVals; i++) {
      vals[cpu].push_back(mp.allocate_on_cpu<Foo>(cpu, static_cast<int>(i),
                                                  static_cast<int>(i + cpu)));
    }
  }

  for (size_t cpu = 0; cpu < std::thread::hardware_concurrency(); cpu++) {
    for (int i = 0; i < kNumVals; i++) {
      EXPECT_EQ(vals[cpu][i]->foo_in_epoch_0, i);
      EXPECT_EQ(vals[cpu][i]->b, i + cpu);
    }
  }
}

TEST(Epoch, delayed_dtor) {
  struct Foo {
    Foo(int d) : data(d) {}

    int data;
  };

  Foo* foo_in_epoch_0;
  Foo* foo_in_epoch_1;
  hsp::HyperSharedPointer<CPULocalMemoryPools> epoch_1_ref;
  {
    auto hptr = Epoch::get_allocator();  // Epoch 0
    foo_in_epoch_0 = hptr->allocate<Foo>(10);
    EXPECT_EQ(foo_in_epoch_0->data, 10);
    Epoch::new_epoch();  // Epoch 1
    epoch_1_ref = Epoch::get_allocator();
    EXPECT_EQ(foo_in_epoch_0->data, 10);
    foo_in_epoch_1 = epoch_1_ref->allocate<Foo>(11);
    EXPECT_EQ(foo_in_epoch_1->data, 11);
  }  // Free last reference to Epoch 0

  // EXPECT_EQ(foo_in_epoch_0->data, 5); // heap-use-after-free
  EXPECT_EQ(foo_in_epoch_1->data, 11);

  Epoch::new_epoch();  // Epoch 2

  Foo* foo_in_epoch_2 = Epoch::get_allocator()->allocate<Foo>(12);
  EXPECT_EQ(foo_in_epoch_1->data, 11);
  EXPECT_EQ(foo_in_epoch_2->data, 12);

  Epoch::new_epoch();  // Epoch 3

  epoch_1_ref.reset();  // Free last reference to Epoch 1, which also frees last reference to Epoch 2

  // EXPECT_EQ(foo_in_epoch_1->data, 11);  // heap-use-after-free
  // EXPECT_EQ(foo_in_epoch_2->data, 12);  // heap-use-after-free
}
}
