option(ENABLE_CPPCHECK "Enable static analysis with cppcheck" OFF)
option(ENABLE_CLANG_TIDY "Enable static analysis with clang-tidy" OFF)
option(ENABLE_INCLUDE_WHAT_YOU_USE "Enable static analysis with include-what-you-use" OFF)

if(ENABLE_CPPCHECK)
  find_program(CPPCHECK cppcheck)
  if(CPPCHECK)
    set(CPPCHECK_EXTRA_ARGS "")
    if(ENABLE_CPPCHECK_INCONCLUSIVE)
      set(CPPCHECK_EXTRA_ARGS "${CPPCHECK_EXTRA_ARGS} --inconclusive")
    endif()
    set(CMAKE_CXX_CPPCHECK
        ${CPPCHECK}
        --error-exitcode=42
        --verbose
        --std=c++17
        --enable=all
        --inline-suppr
        --check-level=exhaustive
        --suppressions-list=${PROJECT_SOURCE_DIR}/.cppcheck_suppressions.txt
        ${CPPCHECK_EXTRA_ARGS})
  else()
    message(SEND_ERROR "cppcheck requested but executable not found")
  endif()
endif()

if(ENABLE_CLANG_TIDY)
  find_program(CLANGTIDY clang-tidy)
  if(CLANGTIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANGTIDY} -extra-arg=-Wno-unknown-warning-option)
  else()
    message(SEND_ERROR "clang-tidy requested but executable not found")
  endif()
endif()

if(ENABLE_INCLUDE_WHAT_YOU_USE)
  find_program(INCLUDE_WHAT_YOU_USE include-what-you-use)
  if(INCLUDE_WHAT_YOU_USE)
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${INCLUDE_WHAT_YOU_USE})
  else()
    message(SEND_ERROR "include-what-you-use requested but executable not found")
  endif()
endif()
