<h1 align="center">Unexceptionally Consistent Set</h1>
<h3 align="center">
Imagine In-Memory Templated Containers<br/>
Being as Consistent as Databases<br/>
</h3>
<br/>

<p align="center">
  <a href="https://discord.gg/njybmcBEay"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/discord.svg" alt="Discord"></a>
	&nbsp;&nbsp;&nbsp;
  <a href="https://www.linkedin.com/company/unum-cloud/"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/linkedin.svg" alt="LinkedIn"></a>
  &nbsp;&nbsp;&nbsp;
  <a href="https://twitter.com/unum_cloud"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/twitter.svg" alt="Twitter"></a>
  &nbsp;&nbsp;&nbsp;
	<a href="https://unum.cloud/post"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/blog.svg" alt="Blog"></a>
	&nbsp;&nbsp;&nbsp;
	<a href="https://github.com/unum-cloud/ucset"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/github.svg" alt="GitHub"></a>
</p>

---

UCSet library provides `std::set`-like class templates for C++, where every operation is `noexcept`, and no update can be leave the container in a partial state.

There are 3 containers to choose from:

* [`consistent_set`][consistent_set]: serializable consistency, fully sorted, based on [`std::set`][stl-set].
* [`consistent_avl`][consistent_avl]: serializable consistency, fully sorted, based on [AVL trees][avl].
* [`versioning_avl`][versioning_avl]: [snapshot isolation][snapshot] via [MVCC][mvcc], fully sorted, based on [AVL trees][avl].

All of them:

* are `noexcept` top to bottom!
* are templated, to be used with any `noexcept`-movable and `default`-constructible types.
* can be wrapped into [`locked_gt`][locked], to make them thread-safe.
* can be wrapped into [`partitioned_gt`][partitioned], to make them concurrent.

...if you are feeling crazy and want your exceptions and classical interfaces back, you can also wrap any container into [`crazy_gt`][crazy].

## Installation & Usage

The entire library is header-only.
You can just copy-paste it, but its not 2022 any more.
To use this with any packaging/build tool you only need C++17 support.

### CMake

```cmake
include(FetchContent)
FetchContent_Declare(
    ucset
    GIT_REPOSITORY https://github.com/unum-cloud/ucset
    GIT_TAG main
    CONFIGURE_COMMAND "" # Nothing to configure, its that simple :)
    BUILD_COMMAND "" # No build needed, UCSet is header-only
)
FetchContent_MakeAvailable(ucset)
include_directories(${consistent_set_SOURCE_DIR})
```

### Conan

In your `conanfile.txt`, just add:

```toml
[requires]
consistent_set
```

## Testing & Performance

All the stress-testing is done via [UKV][ukv].
That library is stuffed with unit-, integration-, consistency- and stress-tests.

![UKV Landscape](https://github.com/unum-cloud/ukv/raw/main/assets/charts/Intro.png)

Performance-wise, concurrent versions are generally bottlenecked by "mutexes" you are using.
So we allow different implementations:

* STL: [`std::shared_mutex`][stl-shared_mutex]
* Intel One API: [`tbb::rw_mutex`][tbb] or others

## Why we created this?

Hate for [`std::bad_alloc`](https://en.cppreference.com/w/cpp/memory/new/bad_alloc).
If you consider "Out of Memory" to be an exception, you are always underutilizing your system.
It happened way too many times that a program just crashed when I was only getting to the interesting place.
Especially with:

* [Neo4J][neo4j] and every other JVM-based project.
* With big batch sizes beyond VRAM sizes of GPU when doing ML.

At Unum we live in the conditions where machines can easily have 1 TB of RAM per CPU socket, but it is still at least 100x smaller than the datasets we are trying to swallow.

Furthermore, I have been working on [UKV][ukv] for a while now, as we want to standardize CRUD interfaces across databases.
An we needed features, that are missing from Standard Templates Library containers:

* Accessing the **allocator state** by reference.
* **Reserving** memory for tree nodes before inserting.
* Explicitly traversing trees for **random sampling**.
* **Speed**!

[stl-set]: https://en.cppreference.com/w/cpp/container/set
[stl-shared_mutex]: https://en.cppreference.com/w/cpp/thread/shared_mutex
[avl]: https://en.wikipedia.org/wiki/AVL_tree
[tbb]: https://spec.oneapi.io/versions/latest/elements/oneTBB/source/named_requirements/mutexes/rw_mutex.html#readerwritermutex
[dbms]: https://en.wikipedia.org/wiki/Database
[mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
[neo4j]: http://neo4j.com
[snapshot]: https://jepsen.io/consistency/models/snapshot-isolation

[ukv]: https://github.com/unum-cloud/ukv
[consistent_set]: tree/main/include/ucset/consistent_set.hpp
[consistent_avl]: tree/main/include/ucset/consistent_avl.hpp
[versioning_avl]: tree/main/include/ucset/versioning_avl.hpp
[locked]: tree/main/include/ucset/locked.hpp
[partitioned]: tree/main/include/ucset/partitioned.hpp
[crazy]: tree/main/include/ucset/crazy.hpp
