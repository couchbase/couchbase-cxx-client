option(ENABLE_CACHE "Enable cache if available" ON)
if(NOT ENABLE_CACHE)
  return()
endif()

if(NOT DEFINED CACHE_OPTION)
  set(CACHE_OPTION "")
endif()

set(CACHE_OPTION_VALUES "sccache" "ccache")

if(CACHE_OPTION STREQUAL "")
  find_program(SCCACHE_BIN sccache)
  if(SCCACHE_BIN)
    set(CACHE_OPTION "sccache")
    set(CACHE_BINARY ${SCCACHE_BIN})
  else()
    find_program(CCACHE_BIN ccache)
    if(CCACHE_BIN)
      set(CACHE_OPTION "ccache")
      set(CACHE_BINARY ${CCACHE_BIN})
    else()
      set(CACHE_OPTION "none")
      set(CACHE_BINARY "")
    endif()
  endif()
else()
  if(NOT
     CACHE_OPTION
     IN_LIST
     CACHE_OPTION_VALUES)
    message(
      STATUS
        "Using custom compiler cache system: '${CACHE_OPTION}', explicitly supported entries are ${CACHE_OPTION_VALUES}"
    )
  endif()
  find_program(CACHE_BINARY ${CACHE_OPTION})
endif()

if(CACHE_OPTION STREQUAL "none" OR CACHE_BINARY STREQUAL "")
  message(WARNING "No compiler cache found or enabled; building without compiler cache")
else()
  if(CMAKE_GENERATOR MATCHES "Visual Studio")
    message(WARNING "Compiler cache (sccache/ccache) ignored with Visual Studio generator + MSVC. Use -GNinja instead.")
    return()
  endif()

  message(STATUS "Compiler cache '${CACHE_OPTION}' found at: ${CACHE_BINARY}")
  set(CMAKE_CXX_COMPILER_LAUNCHER ${CACHE_BINARY})
  set(CMAKE_C_COMPILER_LAUNCHER ${CACHE_BINARY})
  if(MSVC AND CMAKE_GENERATOR STREQUAL "Ninja")
    set(CMAKE_C_FLAGS_DEBUG "/Z7 /Od /RTC1 /FS")
    set(CMAKE_CXX_FLAGS_DEBUG "/Z7 /Od /RTC1 /FS")
    set(CMAKE_C_FLAGS_RELWITHDEBINFO "/Z7 /O2 /FS")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Z7 /O2 /FS")
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
  endif()
endif()
