cmake_minimum_required (VERSION 3.24)

add_library (
    Phantom.System
    STATIC
    "async_reader_writer_lock.h"
    "async_utility.h"
    "async_value_source.h"
    "concepts.h" 
    "crc32.h" 
    "empty.cpp"
    "encompassing_pending_task.h"
    "functional.h"
    "hash.h"
    "lifetime_tracker.h" 
    "merge.h"
    "pooled_ptr.h" 
    "put_back.h"
    "scope.h"
    "service_provider.h"
    "single_pending_task.h"
    "stable_random.h"
    "ThreadingProvider.cpp"
    "ThreadingProvider.h" 
    "utility.h" 
 )

target_link_libraries(
    Phantom.System
    Phantom.Coroutines
    Phantom.Coroutines.cppcoro
)

target_include_directories(
    Phantom.System
    PUBLIC
    ..
)
