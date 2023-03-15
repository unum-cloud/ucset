#include <benchmark/benchmark.h>

#include <ucset/consistent_set.hpp>
#include <ucset/consistent_avl.hpp>
#include <ucset/locked.hpp>
#include <ucset/partitioned.hpp>

namespace bm = benchmark;
using namespace unum::ucset;

struct pair_t {
    std::size_t key;
    std::size_t value;

    pair_t(std::size_t key = 0, std::size_t value = 0) noexcept : key(key), value(value) {}
    explicit operator std::size_t() const noexcept { return key; }
    operator bool() const noexcept { return key != -1; }
};

struct pair_compare_t {
    using value_type = std::size_t;
    bool operator()(pair_t a, pair_t b) const noexcept { return a.key < b.key; }
    bool operator()(std::size_t a, pair_t b) const noexcept { return a < b.key; }
    bool operator()(pair_t a, std::size_t b) const noexcept { return a.key < b; }
};

using stl_t = consistent_set_gt<pair_t, pair_compare_t>;
using avl_t = consistent_avl_gt<pair_t, pair_compare_t>;

template <typename cont_at>
void upsert(bm::State& s, cont_at& cont, std::size_t upsert_count = 10'000'000) {

    status_t status;
    if (s.thread_index() == 0)
        status = cont.clear();

    std::size_t fails = 0;
    auto batch_size = upsert_count / s.threads();
    auto offset = s.thread_index() * batch_size;
    auto txn = cont.transaction().value();
    for (auto _ : s) {
        for (std::size_t i = offset; i != offset + batch_size; ++i)
            status = txn.upsert(pair_t(i, i));
        status = txn.stage();
        status = txn.commit();
    }

    s.counters["upsert/s"] = bm::Counter(s.iterations() * batch_size, bm::Counter::kIsRate);
}

int main(int argc, char** argv) {

    auto locked_set = *locked_gt<stl_t>::make();
    auto locked_avl = *locked_gt<stl_t>::make();
    auto partitioned_set = *locked_gt<stl_t>::make();
    auto partitioned_avl = *locked_gt<stl_t>::make();

    bm::RegisterBenchmark("upsert locked set", [&](bm::State& s) { upsert(s, locked_set); })->Threads(4);
    bm::RegisterBenchmark("upsert locked set", [&](bm::State& s) { upsert(s, locked_set); })->Threads(8);
    bm::RegisterBenchmark("upsert locked set", [&](bm::State& s) { upsert(s, locked_set); })->Threads(16);

    bm::RegisterBenchmark("upsert locked avl", [&](bm::State& s) { upsert(s, locked_avl); })->Threads(4);
    bm::RegisterBenchmark("upsert locked avl", [&](bm::State& s) { upsert(s, locked_avl); })->Threads(8);
    bm::RegisterBenchmark("upsert locked avl", [&](bm::State& s) { upsert(s, locked_avl); })->Threads(16);

    bm::RegisterBenchmark("upsert partitioned set", [&](bm::State& s) { upsert(s, partitioned_set); })->Threads(4);
    bm::RegisterBenchmark("upsert partitioned set", [&](bm::State& s) { upsert(s, partitioned_set); })->Threads(8);
    bm::RegisterBenchmark("upsert partitioned set", [&](bm::State& s) { upsert(s, partitioned_set); })->Threads(16);

    bm::RegisterBenchmark("upsert partitioned avl", [&](bm::State& s) { upsert(s, partitioned_avl); })->Threads(4);
    bm::RegisterBenchmark("upsert partitioned avl", [&](bm::State& s) { upsert(s, partitioned_avl); })->Threads(8);
    bm::RegisterBenchmark("upsert partitioned avl", [&](bm::State& s) { upsert(s, partitioned_avl); })->Threads(16);
    bm::RunSpecifiedBenchmarks();
}