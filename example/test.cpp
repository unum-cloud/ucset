#include <iostream>
#include <cstdlib>
#include <ctime>

#include <ucset/consistent_set.hpp>
#include <gtest/gtest.h>

using namespace unum::ucset;

constexpr std::size_t size = 1'000'000;

struct pair_t {
    std::size_t key;
    std::size_t value;

    pair_t(std::size_t key = 0, std::size_t value = 0) noexcept : key(key), value(value) {}
    explicit operator std::size_t() const noexcept { return key; }
    operator bool() const noexcept { return key; }
};

struct pair_compare_t {
    using value_type = std::size_t;
    bool operator()(pair_t a, pair_t b) const noexcept { return a.key < b.key; }
    bool operator()(std::size_t a, pair_t b) const noexcept { return a < b.key; }
    bool operator()(pair_t a, std::size_t b) const noexcept { return a.key < b; }
};
using cont_t = consistent_set_gt<pair_t, pair_compare_t>;
using identifier_t = typename cont_t::identifier_t;

TEST(upsert_and_find, ascending){
    auto set = *cont_t::make();

    for(std::size_t idx = 0; idx < size; ++idx){
        set.upsert(pair_t{idx, idx});
        EXPECT_TRUE(set.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(set.size(), size);
}

TEST(upsert_and_find, descending){
    auto set = *cont_t::make();

    for(std::size_t idx = size; idx > 0; --idx){
        set.upsert(pair_t{idx, idx});
        EXPECT_TRUE(set.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(set.size(), size);
}

TEST(upsert_and_find, random){
    std::srand(std::time(nullptr));
    auto set = *cont_t::make();

    for(std::size_t idx = 0; idx < size; ++idx){
        std::size_t val = std::rand();
        set.upsert(pair_t{val, val});
        EXPECT_TRUE(set.find(val, [](pair_t const&) noexcept {}));
    }
}

int main(int argc, char** argv){
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}