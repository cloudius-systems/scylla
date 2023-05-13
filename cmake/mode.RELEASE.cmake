set(Seastar_OptimizationLevel_RELEASE "3")
set(CMAKE_CXX_FLAGS_RELEASE
  "-ffunction-sections -fdata-sections"
  CACHE
  INTERNAL
  "")
string(APPEND CMAKE_CXX_FLAGS_RELEASE
  " -O${Seastar_OptimizationLevel_RELEASE}")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
  set(clang_inline_threshold 300)
else()
  set(clang_inline_threshold 2500)
endif()
string(APPEND CMAKE_CXX_FLAGS_RELEASE
  " $<$<CXX_COMPILER_ID:GNU>:--param inline-unit-growth=300"
  " $<$<CXX_COMPILER_ID:Clang>:-mllvm -inline-threshold=${clang_inline_threshold}>"
  # clang generates 16-byte loads that break store-to-load forwarding
  # gcc also has some trouble: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103554
  " -fno-slp-vectorize")
set(Seastar_DEFINITIONS_DEBUG
  SCYLLA_BUILD_MODE=release)

set(CMAKE_STATIC_LINKER_FLAGS_RELEASE
  "-Wl,--gc-sections")

set(stack_usage_threshold_in_KB 13)

include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported OUTPUT error)
if(ipo_supported)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()
