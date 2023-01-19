#include <iostream>
#include <cstdlib>
#include <ctime>

#include <ucset/consistent_set.hpp>
#include <ucset/consistent_avl.hpp>
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
using stl_t = consistent_set_gt<pair_t, pair_compare_t>;
using avl_t = consistent_avl_gt<pair_t, pair_compare_t>;
using id_stl_t = typename stl_t::identifier_t;
using id_avl_t = typename avl_t::identifier_t;

TEST(upsert_and_find_set, ascending){
    auto set = *stl_t::make();

    for(std::size_t idx = 0; idx < size; ++idx) {
        set.upsert(pair_t{idx, idx});
        EXPECT_TRUE(set.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(set.size(), size);
}

TEST(upsert_and_find_set, descending){
    auto set = *stl_t::make();

    for(std::size_t idx = size; idx > 0; --idx) {
        set.upsert(pair_t{idx, idx});
        EXPECT_TRUE(set.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(set.size(), size);
}

TEST(upsert_and_find_set, random){
    std::srand(std::time(nullptr));
    auto set = *stl_t::make();

    for(std::size_t idx = 0; idx < size; ++idx) {
        std::size_t val = std::rand();
        set.upsert(pair_t{val, val});
        EXPECT_TRUE(set.find(val, [](pair_t const&) noexcept {}));
    }
}

TEST(upsert_and_find_avl, ascending){
    auto avl = *avl_t::make();

    for(std::size_t idx = 0; idx < size; ++idx) {
        avl.upsert(pair_t{idx, idx});
        EXPECT_TRUE(avl.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(avl.size(), size);
}

TEST(upsert_and_find_avl, descending){
    auto avl = *avl_t::make();

    for(std::size_t idx = size; idx > 0; --idx) {
        avl.upsert(pair_t{idx, idx});
        EXPECT_TRUE(avl.find(idx, [](pair_t const&) noexcept {}));
    }
    EXPECT_EQ(avl.size(), size);
}

TEST(upsert_and_find_avl, random){
    std::srand(std::time(nullptr));
    auto avl = *avl_t::make();

    for(std::size_t idx = 0; idx < size; ++idx) {
        std::size_t val = std::rand();
        avl.upsert(pair_t{val, val});
        EXPECT_TRUE(avl.find(val, [](pair_t const&) noexcept {}));
    }
}

// TEST(test_set, erase){
//     auto set = *stl_t::make();

//     for(std::size_t idx = 0; idx < size; ++idx)
//         set.upsert(pair_t{idx, idx});

//     for(size_t idx = 0; idx < size; ++idx) {
//         std::cout << idx << "  ";
//         EXPECT_TRUE(set.erase_range(idx, idx + 1, [](pair_t const&) noexcept {}));
//         EXPECT_FALSE(set.find(idx, [](pair_t const&) noexcept {}));
//     }
// }

TEST(test_set, reserve_clear){
    auto set = *stl_t::make();
    EXPECT_TRUE(set.reserve(size));
    EXPECT_EQ(set.size(),0);

    for(std::size_t idx = 0; idx < size; ++idx)
        set.upsert(pair_t{idx, idx});

    EXPECT_EQ(set.size(),size);
    EXPECT_TRUE(set.clear());
    EXPECT_EQ(set.size(),0);
}

TEST(test_avl, clear){
    auto avl = *avl_t::make();
    EXPECT_EQ(avl.size(),0);

    for(std::size_t idx = 0; idx < size; ++idx)
        avl.upsert(pair_t{idx, idx});

    EXPECT_EQ(avl.size(),size);
    EXPECT_TRUE(avl.clear());
    EXPECT_EQ(avl.size(),0);
}

int main(int argc, char** argv){
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}