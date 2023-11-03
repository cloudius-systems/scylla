if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  # -fasan -Og breaks some coroutines on aarch64, use -O0 instead
  set(default_Seastar_OptimizationLevel_DEBUG "0")
else()
  set(default_Seastar_OptimizationLevel_DEBUG "g")
endif()
set(Seastar_OptimizationLevel_DEBUG
  ${default_Seastar_OptimizationLevel_DEBUG}
  CACHE
  INTERNAL
  "")

set(Seastar_DEFINITIONS_DEBUG
  SCYLLA_BUILD_MODE=debug
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_DEBUG})
  add_compile_definitions(
    $<$<CONFIG:Debug>:${definition}>)
endforeach()

set(CMAKE_CXX_FLAGS_DEBUG
  " -O${Seastar_OptimizationLevel_DEBUG} -g -gz")

set(stack_usage_threshold_in_KB 40)
