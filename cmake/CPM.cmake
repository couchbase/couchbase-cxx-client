set(CPM_DOWNLOAD_VERSION 0.43.1)
# Published by upstream in the get_cpm.cmake of the same release. Update both together.
set(CPM_HASH_SUM "1c40fc102ce9625d7de7eb14f541cab30cc3138dca627f0b0ec40293ce6c2934")

if(CPM_SOURCE_CACHE)
  set(CPM_DOWNLOAD_LOCATION "${CPM_SOURCE_CACHE}/cpm/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
elseif(DEFINED ENV{CPM_SOURCE_CACHE})
  set(CPM_DOWNLOAD_LOCATION "$ENV{CPM_SOURCE_CACHE}/cpm/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
else()
  set(CPM_DOWNLOAD_LOCATION "${CMAKE_BINARY_DIR}/cmake/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
endif()

# Expand relative path. This is important if the provided path contains a tilde (~)
get_filename_component(CPM_DOWNLOAD_LOCATION ${CPM_DOWNLOAD_LOCATION} ABSOLUTE)

set(CPM_DOWNLOAD_ATTEMPTS
    5
    CACHE STRING "How many times to try downloading CPM.cmake before giving up")
set(CPM_DOWNLOAD_RETRY_DELAY
    2
    CACHE STRING "Seconds to wait after the first failed attempt; each further wait doubles")
# Without a bound, a peer that accepts the connection and then stops sending leaves file(DOWNLOAD)
# waiting indefinitely and the retry below never runs. INACTIVITY_TIMEOUT bounds a stall, TIMEOUT
# the whole transfer; the file is small, so both can be short enough to retry rather than hang.
set(CPM_DOWNLOAD_INACTIVITY_TIMEOUT
    20
    CACHE STRING "Seconds of no data before an attempt to download CPM.cmake is abandoned")
set(CPM_DOWNLOAD_TIMEOUT
    60
    CACHE STRING "Seconds before an attempt to download CPM.cmake is abandoned outright")

# A file at CPM_DOWNLOAD_LOCATION is not necessarily CPM: file(DOWNLOAD) reports transport errors
# only, so an HTTP 5xx or a rate-limit page arrives as a successful download whose body is HTML.
# Including that defines nothing, and every dependency then fails with "Unknown CMake command
# cpmaddpackage", which names neither the download nor the file to delete before retrying.
set(CPM_VALIDATE_SCRIPT "${CMAKE_BINARY_DIR}/cmake/cpm_validate_candidate.cmake")
file(
  WRITE "${CPM_VALIDATE_SCRIPT}"
  "include(\"\${CPM_CANDIDATE}\")
if(NOT COMMAND cpmaddpackage)
  message(FATAL_ERROR \"\${CPM_CANDIDATE} does not define CPMAddPackage\")
endif()
")

# Whether a candidate is CPM is decided by a separate CMake process that includes it and looks for
# the command. Searching the text for a name would accept a truncated script or an error page that
# merely mentions it, and finding out by including it here would abort this configure on a parse
# error instead of moving on to the next attempt.
function(cpm_file_is_usable path out_var)
  set(${out_var} FALSE PARENT_SCOPE)
  if(NOT EXISTS "${path}")
    return()
  endif()
  execute_process(
    COMMAND "${CMAKE_COMMAND}" "-DCPM_CANDIDATE=${path}" -P "${CPM_VALIDATE_SCRIPT}"
    RESULT_VARIABLE _cpm_validate_result
    OUTPUT_QUIET ERROR_QUIET)
  if(_cpm_validate_result EQUAL 0)
    set(${out_var} TRUE PARENT_SCOPE)
  endif()
endfunction()

# Transient failures fetching this file break every job that configures, so retry rather than fail
# the build on one bad response.
#
# CPM_SOURCE_CACHE is shared between build trees, so nothing here may write or delete the shared path
# while another configure could be reading it. Each attempt downloads to a private path, and only a
# candidate that validates is moved onto the shared one; file(RENAME) within a directory is atomic, so
# a concurrent reader sees either the previous file or the complete new one, never a partial write.
# The private path is derived from the binary directory, which is what distinguishes one configure
# from another.
function(download_cpm)
  set(_url
      "https://github.com/cpm-cmake/CPM.cmake/releases/download/v${CPM_DOWNLOAD_VERSION}/CPM.cmake")
  string(MD5 _tag "${CMAKE_BINARY_DIR}")
  set(_staged "${CPM_DOWNLOAD_LOCATION}.${_tag}.part")
  message(STATUS "Downloading CPM.cmake v${CPM_DOWNLOAD_VERSION} to ${CPM_DOWNLOAD_LOCATION}")
  foreach(_attempt RANGE 1 ${CPM_DOWNLOAD_ATTEMPTS})
    # LOG captures the transfer log, which carries the status line. Without it every HTTP failure
    # reports libcurl's "HTTP response code said error" and nothing distinguishes a rate limit from a
    # missing release, which are the same message and completely different problems.
    # What arrives here is executed, so it is checked before it can be: EXPECTED_HASH against the
    # sum upstream publishes, and TLS_VERIFY because CMake only began verifying certificates by
    # default in 3.31 and this project supports 3.19, so on most of the roots it builds on the
    # connection would otherwise go unauthenticated.
    #
    # The hash applies to the download alone. A cached file is deliberately not checked against it:
    # the source tarball ships a patched CPM (see the sed in cmake/Packaging.cmake), so an offline
    # build from the tarball has a legitimate copy that will never match upstream's sum.
    file(DOWNLOAD "${_url}" "${_staged}" STATUS _status LOG _log
         EXPECTED_HASH SHA256=${CPM_HASH_SUM} TLS_VERIFY ON
         INACTIVITY_TIMEOUT ${CPM_DOWNLOAD_INACTIVITY_TIMEOUT} TIMEOUT ${CPM_DOWNLOAD_TIMEOUT})
    list(GET _status 0 _code)
    if(_code EQUAL 0)
      cpm_file_is_usable("${_staged}" _usable)
      if(_usable)
        file(RENAME "${_staged}" "${CPM_DOWNLOAD_LOCATION}")
        return()
      endif()
      set(_reason "the downloaded file does not define CPMAddPackage")
    else()
      list(GET _status 1 _reason)
      # The last status line wins: a redirect to the asset host means the log holds one per hop.
      string(REGEX MATCHALL "HTTP/[0-9.]+ [0-9]+[^\n\r]*" _http_lines "${_log}")
      if(_http_lines)
        list(POP_BACK _http_lines _http_line)
        string(STRIP "${_http_line}" _http_line)
        set(_reason "${_reason} [${_http_line}]")
      endif()
    endif()
    file(REMOVE "${_staged}")
    if(_attempt LESS CPM_DOWNLOAD_ATTEMPTS)
      # Back off rather than retrying at a fixed interval: the failures worth retrying are a host
      # rate-limiting or shedding load, and a burst of evenly spaced attempts is what it is shedding.
      math(EXPR _delay "${CPM_DOWNLOAD_RETRY_DELAY} * (1 << (${_attempt} - 1))")
      message(STATUS "  attempt ${_attempt} of ${CPM_DOWNLOAD_ATTEMPTS} failed: ${_reason}; "
                     "retrying in ${_delay}s")
      execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep ${_delay})
    else()
      message(STATUS "  attempt ${_attempt} of ${CPM_DOWNLOAD_ATTEMPTS} failed: ${_reason}; "
                     "no attempts left")
    endif()
  endforeach()
  message(
    FATAL_ERROR
      "Could not download CPM.cmake v${CPM_DOWNLOAD_VERSION} from ${_url} in ${CPM_DOWNLOAD_ATTEMPTS} attempts: "
      "${_reason}. Set CPM_SOURCE_CACHE to a directory that already holds "
      "cpm/CPM_${CPM_DOWNLOAD_VERSION}.cmake to build without network access.")
endfunction()

cpm_file_is_usable("${CPM_DOWNLOAD_LOCATION}" CPM_DOWNLOAD_USABLE)
if(NOT CPM_DOWNLOAD_USABLE)
  download_cpm()
endif()

include(${CPM_DOWNLOAD_LOCATION})

# The file validated a moment ago in another process, so reaching this means it was replaced in
# between. Report it rather than letting the first dependency fail with an unexplained missing
# command, and do not delete it: the writer was another configure, whose file this run has no claim
# on, and the check above rejects it on the next run anyway if it really is unusable.
if(NOT COMMAND cpmaddpackage)
  message(FATAL_ERROR "${CPM_DOWNLOAD_LOCATION} did not define CPMAddPackage; it was replaced while this "
                      "configure was reading it. Re-run to validate and download it again.")
endif()
