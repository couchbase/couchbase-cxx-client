# Post-build enumerator for couchbase_discover_tests(). Runs the freshly linked test executable
# with --list-tests and writes one add_test() per case, so ctest sees the same granularity
# catch_discover_tests gives the Catch2 suites: `ctest -I i,,n` can shard a binary, `-R` can select
# a single case, and --output-junit carries one row per case.
#
# Driven by cmake -P with TEST_TARGET, TEST_EXECUTABLE, TEST_PROPERTIES and CTEST_FILE defined.

execute_process(
  COMMAND "${TEST_EXECUTABLE}" --list-tests
  OUTPUT_VARIABLE listing
  ERROR_VARIABLE listing_error
  RESULT_VARIABLE listing_result)

if(NOT listing_result EQUAL 0)
  message(FATAL_ERROR "${TEST_EXECUTABLE} --list-tests failed (${listing_result}):\n${listing_error}")
endif()

# Splitting on newlines below makes ";" the list separator, so any semicolon already in the text
# has to be escaped first or one line becomes two entries. Case names cannot contain one -- they are
# C++ identifiers, see the naming rule in couchbase_discover_tests() -- so this guards whatever else
# the listing carries beside the name.
string(REPLACE ";" "\\;" listing "${listing}")
string(REPLACE "\n" ";" listing "${listing}")

# Bracket-quoted once, then expanded unquoted at ctest time: interpolating the list straight into
# the file splits it on any space inside a value -- a TSan suppressions path containing one would
# have silently truncated the ENVIRONMENT property rather than failing.
set(script "set(properties [==[${TEST_PROPERTIES}]==])\n")
set(case_count 0)
foreach(line IN LISTS listing)
  # Each line is "<case name>\t<what it requires>". Only the name is registered; the requirements
  # are there for whoever runs --list-tests by hand.
  string(REGEX REPLACE "\t.*$" "" case_name "${line}")
  string(STRIP "${case_name}" case_name)
  if(case_name STREQUAL "")
    continue()
  endif()
  math(EXPR case_count "${case_count} + 1")
  set(test_name "${TEST_TARGET}.${case_name}")
  string(APPEND script "add_test([==[${test_name}]==] [==[${TEST_EXECUTABLE}]==] [==[${case_name}]==])\n")
  string(APPEND script "set_tests_properties([==[${test_name}]==] PROPERTIES \${properties})\n")
endforeach()

# A binary that reports no cases has to break the build. Writing an empty file instead would leave
# ctest green with nothing registered, which is the failure this whole scheme exists to make
# impossible.
if(case_count EQUAL 0)
  message(FATAL_ERROR "${TEST_EXECUTABLE} --list-tests reported no test cases")
endif()

file(WRITE "${CTEST_FILE}" "${script}")
