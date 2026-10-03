cmake_minimum_required(VERSION 3.16)
project(taskforge CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(TASKFORGE_SANITIZE "thread | address | undefined | (empty)" "")
if(TASKFORGE_SANITIZE)
  add_compile_options(-fsanitize=${TASKFORGE_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${TASKFORGE_SANITIZE})
endif()
add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

# ---- core library ----------------------------------------------------------
add_library(taskforge_core src/thread_pool.cpp)
target_include_directories(taskforge_core PUBLIC include)
target_link_libraries(taskforge_core PUBLIC Threads::Threads)

# ---- PostgreSQL layer ------------------------------------------------------
find_package(PostgreSQL REQUIRED)
add_library(taskforge_db src/db.cpp src/job_store.cpp)
target_include_directories(taskforge_db PUBLIC include ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(taskforge_db PUBLIC ${PostgreSQL_LIBRARIES} taskforge_core)

# ---- engine: scheduler + handlers -------------------------------------------
find_package(nlohmann_json REQUIRED)
add_library(taskforge_engine src/scheduler.cpp src/handlers.cpp src/stb_impl.cpp src/metrics_server.cpp)
target_include_directories(taskforge_engine PUBLIC include)
target_include_directories(taskforge_engine SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/../third_party)
set_source_files_properties(src/stb_impl.cpp PROPERTIES COMPILE_OPTIONS "-w")
target_link_libraries(taskforge_engine PUBLIC taskforge_db nlohmann_json::nlohmann_json)

add_executable(taskforge_scheduler src/scheduler_main.cpp)
target_link_libraries(taskforge_scheduler PRIVATE taskforge_engine)

# ---- tests (GoogleTest: system package, else fetched) ----------------------
# Gated behind an option (default ON) so a production image build -- which only ever
# needs `--target taskforge_scheduler` -- can pass -DTASKFORGE_BUILD_TESTS=OFF and skip
# this whole section. Without that gate, CMake's *configure* step processes this file
# unconditionally regardless of which target is later built, so every Docker build
# would fetch and configure GoogleTest from GitHub for a binary that never uses it --
# confirmed by actually building cpp/Dockerfile's exact package list + commands in a
# fresh debootstrap chroot with no libgtest-dev installed: the fetch happened, and
# build/_deps/googletest-src appeared, purely from running the configure step.
option(TASKFORGE_BUILD_TESTS "Build the GoogleTest suites (needs GTest, network if not installed)" ON)
if(TASKFORGE_BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip)
    FetchContent_MakeAvailable(googletest)
  endif()
  include(GoogleTest)

  add_executable(thread_pool_test tests/thread_pool_test.cpp)
  target_link_libraries(thread_pool_test PRIVATE taskforge_core GTest::gtest_main)
  gtest_discover_tests(thread_pool_test)

  add_executable(job_store_test tests/job_store_test.cpp)
  target_link_libraries(job_store_test PRIVATE taskforge_db GTest::gtest_main)
  gtest_discover_tests(job_store_test)

  add_executable(metrics_server_test tests/metrics_server_test.cpp)
  target_link_libraries(metrics_server_test PRIVATE taskforge_engine GTest::gtest_main)
  gtest_discover_tests(metrics_server_test)

  add_executable(handlers_test tests/handlers_test.cpp)
  target_include_directories(handlers_test SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/../third_party)
  target_link_libraries(handlers_test PRIVATE taskforge_engine GTest::gtest_main)
  gtest_discover_tests(handlers_test)

  add_executable(scheduler_test tests/scheduler_test.cpp)
  target_link_libraries(scheduler_test PRIVATE taskforge_engine GTest::gtest_main)
  gtest_discover_tests(scheduler_test)
endif()

# ---- examples --------------------------------------------------------------
add_executable(pool_bench examples/pool_bench.cpp)
target_link_libraries(pool_bench PRIVATE taskforge_core)

add_executable(claim_bench examples/claim_bench.cpp)
target_link_libraries(claim_bench PRIVATE taskforge_db)
