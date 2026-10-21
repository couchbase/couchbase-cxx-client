# Writes the Apple linker's -unexported_symbols_list for the statically linked AWS-LC: every
# external symbol its archives define, as nm prints it (Mach-O names keep their leading underscore).
# The Apple linker has no --exclude-libs, and a list of name prefixes misses names such as SHA256 or
# X25519, so the list is read from the archives themselves.
#
#   cmake -DNM=<nm> -DCRYPTO=<libcrypto.a> -DSSL=<libssl.a> -DOUTPUT=<file> -P UnexportedTlsSymbols.cmake
#
# Standard library code is left out: instantiations in namespace std and the global operator new and
# delete, which both the archives and the client define inline. They are not TLS names.
# bin/check-exported-symbols applies the same rule.

foreach(var NM CRYPTO SSL OUTPUT)
  if(NOT ${var})
    message(FATAL_ERROR "UnexportedTlsSymbols.cmake: -D${var}=... is required")
  endif()
endforeach()

execute_process(
  COMMAND "${NM}" -g --defined-only "${CRYPTO}" "${SSL}"
  RESULT_VARIABLE nm_result
  OUTPUT_VARIABLE nm_output
  ERROR_VARIABLE nm_error)
if(NOT nm_result EQUAL 0)
  message(FATAL_ERROR "${NM} -g --defined-only failed (${nm_result}): ${nm_error}")
endif()

string(REPLACE "\n" ";" nm_lines "${nm_output}")
set(names)
foreach(line IN LISTS nm_lines)
  if(line MATCHES "^[0-9a-fA-F]* *[A-Za-z] ([^ ]+)$")
    set(name "${CMAKE_MATCH_1}")
    if(NOT name MATCHES "^_?_Z((NK?|T[VIS]N?)?St|nw|na|dl|da)")
      list(APPEND names "${name}")
    endif()
  endif()
endforeach()
list(REMOVE_DUPLICATES names)

# An empty list hides nothing and would link cleanly, so it is an error rather than a result.
if(NOT names)
  message(FATAL_ERROR "${NM} reported no defined external symbols in ${CRYPTO} and ${SSL}")
endif()

string(JOIN "\n" content ${names})
file(WRITE "${OUTPUT}" "${content}\n")
