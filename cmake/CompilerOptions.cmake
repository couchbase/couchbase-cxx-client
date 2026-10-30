include(CMakePushCheckState)
include(CheckSymbolExists)

cmake_push_check_state(RESET)
find_library(EXECINFO_LIBRARY NAMES execinfo)
if(EXECINFO_LIBRARY)
  set(CMAKE_REQUIRED_LIBRARIES "${EXECINFO_LIBRARY}")
  list(APPEND PLATFORM_LIBRARIES "${EXECINFO_LIBRARY}")
endif(EXECINFO_LIBRARY)
check_symbol_exists(backtrace execinfo.h HAVE_BACKTRACE)
cmake_pop_check_state()

cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
if(CMAKE_DL_LIBS)
  set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_DL_LIBS}")
  list(APPEND PLATFORM_LIBRARIES ${CMAKE_DL_LIBS})
endif()
check_symbol_exists(dladdr dlfcn.h HAVE_DLADDR)
cmake_pop_check_state()

option(ENABLE_BUILD_WITH_TIME_TRACE "Enable -ftime-trace to generate time tracing .json files on clang" OFF)

function(set_project_options target_name)
  if(ENABLE_BUILD_WITH_TIME_TRACE)
    target_compile_options(${target_name} PRIVATE -ftime-trace)
  endif()

  target_compile_features(${target_name} PRIVATE cxx_std_17)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /bigobj /FS)
    target_link_libraries(${target_name} PRIVATE iphlpapi)
  else()
    target_compile_options(${target_name} PRIVATE $<$<COMPILE_LANGUAGE:C,CXX>:-ffunction-sections>
                                                  $<$<COMPILE_LANGUAGE:C,CXX>:-fdata-sections>)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
      get_target_property(_target_type ${target_name} TYPE)
      if(_target_type STREQUAL "SHARED_LIBRARY" OR _target_type STREQUAL "EXECUTABLE")
        target_link_options(${target_name} PRIVATE -Wl,--gc-sections)
      endif()
    endif()
  endif()

  if(HAVE_BACKTRACE)
    target_compile_definitions(${target_name} PRIVATE -DHAVE_BACKTRACE=1)
  endif()

  if(HAVE_DLADDR)
    target_compile_definitions(${target_name} PRIVATE -DHAVE_DLADDR=1)
  endif()

  if(HAVE_BACKTRACE OR HAVE_DLADDR)
    target_compile_definitions(${target_name} PRIVATE -D_GNU_SOURCE=1)
    # print_backtrace() names a frame through dladdr(), which reads only .dynsym. An executable puts
    # its own functions there only when linked with -rdynamic; a shared library exports without it.
    get_target_property(_target_type ${target_name} TYPE)
    if(_target_type STREQUAL "EXECUTABLE")
      target_link_options(${target_name} PRIVATE -rdynamic)
    endif()
  endif()
endfunction()

function(couchbase_get_targets_recursively out_var dir)
  get_property(
    _targets
    DIRECTORY "${dir}"
    PROPERTY BUILDSYSTEM_TARGETS)
  get_property(
    _subdirs
    DIRECTORY "${dir}"
    PROPERTY SUBDIRECTORIES)
  foreach(_subdir IN LISTS _subdirs)
    couchbase_get_targets_recursively(_sub_targets "${_subdir}")
    list(APPEND _targets ${_sub_targets})
  endforeach()
  set(${out_var}
      "${_targets}"
      PARENT_SCOPE)
endfunction()

function(couchbase_optimize_third_party_target target_name)
  if(NOT MSVC AND TARGET ${target_name})
    get_target_property(_target_type ${target_name} TYPE)
    if(_target_type STREQUAL "STATIC_LIBRARY"
       OR _target_type STREQUAL "SHARED_LIBRARY"
       OR _target_type STREQUAL "OBJECT_LIBRARY")
      # -w: diagnostics in code this project does not maintain cannot be acted on here.
      target_compile_options(
        ${target_name} PRIVATE $<$<COMPILE_LANGUAGE:C,CXX>:-ffunction-sections>
                               $<$<COMPILE_LANGUAGE:C,CXX>:-fdata-sections> $<$<COMPILE_LANGUAGE:C,CXX>:-w>)
      if(COUCHBASE_CXX_CLIENT_PACKAGE_BUILD)
        # Lowers the debug level of a build type that has one; never adds debug info to one that has not.
        target_compile_options(
          ${target_name} PRIVATE $<$<AND:$<COMPILE_LANGUAGE:C,CXX>,$<CONFIG:Debug,RelWithDebInfo>>:-g1>)
      endif()
    endif()
  endif()
endfunction()

function(couchbase_optimize_third_party_directory dir)
  if(NOT MSVC AND EXISTS "${dir}")
    couchbase_get_targets_recursively(_third_party_targets "${dir}")
    foreach(_target IN LISTS _third_party_targets)
      couchbase_optimize_third_party_target(${_target})
    endforeach()
  endif()
endfunction()

function(propagate_public_compile_definitions receiver)
  set(receiver_target ${receiver})
  set(donor_targets ${ARGN})

  set(all_defs "")
  foreach(donor IN LISTS donor_targets)
    get_target_property(defs ${donor} INTERFACE_COMPILE_DEFINITIONS)
    if(defs)
      list(APPEND all_defs ${defs})
    endif()
  endforeach()

  list(REMOVE_DUPLICATES all_defs)

  target_compile_definitions(${receiver_target} PUBLIC ${all_defs})
endfunction()
