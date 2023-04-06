#pragma once

namespace theta {

int get_cpu();

class alignas(256) Arena {
 public:
  static ArenaHandle claimArenaHandle();

  void increment(int cpu);

  bool decrement(int originalCpu);

  size_t use_count() const;

 private:
  friend class ArenaHandle;

  static Arena *create();
  static void destroy(Arena *arena);

  // This cannot be constructed because slabs_ will be a properly sized array,
  // which avoid an extra pointer lookup in std::vector.
  Arena() = delete;

  struct alignas(256) {
    std::atomic<uint64_t> usedCpus;
    size_t sizeofArena;
    int numCpus;
  } info_;

  // This will end up having a Slab for each CPU. malloc is used instead of a
  // constructor to make this abomination happen.
  Slab slabs_[1];

  bool markCpu(int cpu);
  uint64_t unmarkCpu(int cpu);

  uint64_t unmarkSlabSlot();
};

class ArenaHandle {
 public:
  ArenaHandle(Arena* arena, int offset);
  ~ArenaHandle();

  Arena* arena() const;
  int offset() const;

 private:
  uint64_t reference_;
};

class CPUShardedCounter {
 public:
};

}  // namespace theta
