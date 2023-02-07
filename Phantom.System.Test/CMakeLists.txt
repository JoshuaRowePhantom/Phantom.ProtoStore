
add_executable(
    Phantom.System.Test
    "ArgumentConverterTests.cpp"
    "as_awaitable_tests.cpp"
    "async_bound_tests.cpp"
    "async_ring_buffer_tests.cpp"
    
    "async_utility_concept_tests.cpp"
    "async_value_source_tests.cpp"
    "encompassing_pending_task_tests.cpp"
    "invoke_as_awaitable_tests.cpp"
    "merge_tests.cpp"
    "move_and_copy_counter.h"
    "pooled_ptr_tests.cpp" 
    "scope_tests.cpp" 
    "service_provider_tests.cpp" 
    "single_pending_task_tests.cpp" 
    "StandardIncludes.h"
    "ThreadingTests.cpp"
    "ThreadingTests.h")

target_link_libraries(
    Phantom.System.Test
    Phantom.System
    GTest::gtest
    GTest::gtest_main
    Phantom.Coroutines
    Phantom.Coroutines.cppcoro
)

enable_testing()

gtest_discover_tests(Phantom.System.Test)
