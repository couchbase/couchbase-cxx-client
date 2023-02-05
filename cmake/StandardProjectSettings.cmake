set(CMAKE_CXX_STANDARD 17)

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE
      RelWithDebInfo
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui, ccmake
  set_property(
    CACHE CMAKE_BUILD_TYPE
    PROPERTY STRINGS
             "Debug"
             "Release"
             "MinSizeRel"
             "RelWithDebInfo")
endif()

# Generate compile_commands.json to make it easier to work with clang based tools
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_IPO "Enable Interprocedural Optimization, aka Link Time Optimization (LTO)" OFF)

if(ENABLE_IPO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT result OUTPUT output)
  if(result)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
  else()
    message(SEND_ERROR "IPO is not supported: ${output}")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  add_compile_options(-fcolor-diagnostics)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options(-fdiagnostics-color=always)
else()
  message(STATUS "No colored compiler diagnostic set for '${CMAKE_CXX_COMPILER_ID}' compiler.")
endif()

if(MSVC)
  add_definitions(/bigobj)

  # /Zc:preprocessor enables a token-based preprocessor that conforms to C99 and C++11 and later standards
  # /Zc:preprocessor option is available starting with VS2019 version 16.5
  # (according to https://docs.microsoft.com/en-us/cpp/build/reference/zc-preprocessor)
  # That version is equivalent to _MSC_VER==1925
  # (according to https://docs.microsoft.com/en-us/cpp/preprocessor/predefined-macros)
  # CMake's ${MSVC_VERSION} is equivalent to _MSC_VER
  # (according to https://cmake.org/cmake/help/latest/variable/MSVC_VERSION.html#variable:MSVC_VERSION)
  if (MSVC_VERSION GREATER_EQUAL 1925)
    add_compile_options(/Zc:preprocessor)
  else()
    message(FATAL_ERROR "MSVC compiler before VS2019 Update5 are not supported")
  endif()
endif()

