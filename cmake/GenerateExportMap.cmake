# Writes the linker version script for the packaged libcouchbase_cxx_client.so (see CMakeLists.txt).
#
# The library exports the public API and nothing else. A global symbol defined by the SDK objects is
# exported when the entity it names
#
#   - lies in namespace couchbase outside couchbase::core, every name in its qualified name appears
#     in the public headers' code, and no enclosing scope is a class the public headers only
#     forward-declare (the *_impl and internal_* classes they hold through std::shared_ptr); or
#   - is one of COUCHBASE_PUBLIC_CORE_FUNCTIONS, which the public headers call inline, so an
#     application's own code refers to them.
#
# Everything else -- couchbase::core, the implementation classes and the vendored libraries -- stays
# local. The script lists exact symbol names, so what the library exports is decided here.
#
# Run via: cmake -D OBJECTS=<file listing the SDK objects> -D HEADERS_DIR=<source>/couchbase
#                -D NM=<nm> -D OUTPUT=<map> -P GenerateExportMap.cmake

# Script mode starts with every policy at OLD; IN_LIST and ZIP_LISTS need the project's.
cmake_minimum_required(VERSION 3.19)

# A public header that starts calling another couchbase::core function inline adds it here, or an
# application using that header fails to link against the packaged library.
set(COUCHBASE_PUBLIC_CORE_FUNCTIONS
    # couchbase/error_codes.hxx: make_error_code()
    couchbase::core::impl::analytics_category
    couchbase::core::impl::common_category
    couchbase::core::impl::field_level_encryption_category
    couchbase::core::impl::key_value_category
    couchbase::core::impl::management_category
    couchbase::core::impl::network_category
    couchbase::core::impl::query_category
    couchbase::core::impl::search_category
    couchbase::core::impl::streaming_json_lexer_category
    couchbase::core::impl::transaction_category
    couchbase::core::impl::transaction_op_category
    couchbase::core::impl::view_category
    # couchbase/*_options.hxx: expiry()
    couchbase::core::impl::expiry_absolute
    couchbase::core::impl::expiry_relative
    # couchbase/codec/raw_*_transcoder.hxx
    couchbase::core::utils::to_binary
    # couchbase/codec/tao_json_serializer.hxx
    couchbase::core::utils::json::generate_binary
    couchbase::core::utils::json::parse_binary)

foreach(_var OBJECTS HEADERS_DIR NM OUTPUT)
  if(NOT ${_var})
    message(FATAL_ERROR "GenerateExportMap.cmake: ${_var} is not set")
  endif()
endforeach()

# Global symbols defined by the objects, as a list; mangled names, or demangled with -C. --no-sort
# keeps both listings in file order, so entries correspond by position.
function(read_symbols out)
  execute_process(
    COMMAND "${NM}" --defined-only --extern-only --no-sort --format=just-symbols ${ARGN} "@${OBJECTS}"
    OUTPUT_VARIABLE _text
    RESULT_VARIABLE _rc)
  if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "GenerateExportMap.cmake: ${NM} failed: ${_rc}")
  endif()
  # Drop the per-file headers and blank lines nm prints between objects.
  string(REGEX REPLACE "(^|\n)[^\n]*:(\n|$)" "\\1" _text "${_text}")
  string(REPLACE ";" "\\;" _text "${_text}")
  string(REPLACE "\n" ";" _text "${_text}")
  list(REMOVE_ITEM _text "")
  set(${out} "${_text}" PARENT_SCOPE)
endfunction()

# The entity a demangled symbol names: the special-name prefix ("typeinfo for", "vtable for", ...),
# template arguments, parameter lists, local-entity braces and a function template's return type are
# removed. A std:: or asio:: template instantiated over a couchbase type therefore names a std:: or
# asio:: entity, and a function-local static names its enclosing function's scope.
function(entity_of out demangled)
  set(_s "${demangled}")
  string(REGEX REPLACE "^(typeinfo name for|typeinfo for|vtable for|VTT for|construction vtable for|guard variable for|TLS init function for|TLS wrapper function for|non-virtual thunk to|virtual thunk to|covariant return thunk to) " "" _s "${_s}")
  # A conversion operator names a type after "operator" ("operator bool() const"), which would read
  # as a return type below.
  string(REGEX REPLACE "operator [^(]+\\(" "operator(" _s "${_s}")
  string(REGEX REPLACE "operator(<<=|>>=|<=>|<<|>>|<=|>=|->\\*|->|<|>)" "operator" _s "${_s}")
  foreach(_pattern "<[^<>]*>" "\\([^()]*\\)" "{[^{}]*}")
    set(_previous "")
    while(NOT _s STREQUAL _previous)
      set(_previous "${_s}")
      string(REGEX REPLACE "${_pattern}" "" _s "${_s}")
    endwhile()
  endforeach()
  string(REGEX REPLACE "( (const|volatile|&|&&|noexcept))+$" "" _s "${_s}")
  string(REGEX REPLACE "^.* " "" _s "${_s}")
  set(${out} "${_s}" PARENT_SCOPE)
endfunction()

# Names the public headers mention, define as a class, and forward-declare, kept as variables so a
# lookup is constant time: _mentioned_<name>, _defined_<name>, _forward_<name>.
file(GLOB_RECURSE _headers "${HEADERS_DIR}/*.hxx")
set(_text "")
foreach(_header IN LISTS _headers)
  file(READ "${_header}" _content)
  string(APPEND _text "${_content}\n")
endforeach()
# A word in a comment does not make a name public.
string(REGEX REPLACE "/\\*([^*]|\\*+[^*/])*\\*+/" " " _text "${_text}")
string(REGEX REPLACE "//[^\n]*" "" _text "${_text}")
string(REGEX MATCHALL "[A-Za-z_][A-Za-z0-9_]*" _names "${_text}")
list(REMOVE_DUPLICATES _names)
foreach(_name IN LISTS _names)
  set(_mentioned_${_name} TRUE)
endforeach()
# An attribute or alignas() may sit between the class-key and the name.
set(_attributes "((\\[\\[[^]]*\\]\\]|alignas\\([^)]*\\))[ \t\n]*)*")
string(REGEX MATCHALL "(class|struct)[ \t\n]+${_attributes}[A-Za-z_][A-Za-z0-9_]*([ \t\n]+final)?[ \t\n]*[:{]" _definitions "${_text}")
foreach(_match IN LISTS _definitions)
  string(REGEX REPLACE "^(class|struct)[ \t\n]+${_attributes}([A-Za-z_][A-Za-z0-9_]*).*" "\\4" _name "${_match}")
  set(_defined_${_name} TRUE)
endforeach()
string(REGEX MATCHALL "(class|struct)[ \t\n]+[A-Za-z_][A-Za-z0-9_]*[ \t\n]*;" _declarations "${_text}")
foreach(_match IN LISTS _declarations)
  string(REGEX REPLACE "^(class|struct)[ \t\n]+([A-Za-z_][A-Za-z0-9_]*).*" "\\2" _name "${_match}")
  set(_forward_${_name} TRUE)
endforeach()

# names_class: the last name is itself a class (typeinfo, vtable, VTT, thunk), not a member.
function(is_exported out entity names_class)
  set(${out} FALSE PARENT_SCOPE)
  if(NOT entity MATCHES "^couchbase::")
    return()
  endif()
  if(entity MATCHES "^couchbase::core::")
    foreach(_function IN LISTS COUCHBASE_PUBLIC_CORE_FUNCTIONS)
      if(entity STREQUAL _function OR entity MATCHES "^${_function}::")
        set(${out} TRUE PARENT_SCOPE)
        return()
      endif()
    endforeach()
    return()
  endif()
  string(REGEX REPLACE "^couchbase::" "" _rest "${entity}")
  string(REPLACE "::" ";" _components "${_rest}")
  list(LENGTH _components _count)
  math(EXPR _last "${_count} - 1")
  set(_index -1)
  foreach(_component IN LISTS _components)
    math(EXPR _index "${_index} + 1")
    string(REGEX REPLACE "^~" "" _component "${_component}")
    string(REGEX REPLACE "\\[abi:[^]]*\\]" "" _component "${_component}")
    if(_component STREQUAL "" OR _component MATCHES "^operator")
      continue()
    endif()
    if(NOT _mentioned_${_component})
      return()
    endif()
    # Only a scope can be a class: a member function may share its name with an unrelated class
    # the headers forward-declare.
    if(_forward_${_component} AND NOT _defined_${_component} AND (_index LESS _last OR names_class))
      return()
    endif()
  endforeach()
  set(${out} TRUE PARENT_SCOPE)
endfunction()

read_symbols(_mangled)
read_symbols(_demangled -C)
list(LENGTH _mangled _count)
list(LENGTH _demangled _demangled_count)
if(NOT _count EQUAL _demangled_count)
  message(FATAL_ERROR "GenerateExportMap.cmake: nm listed ${_count} mangled and ${_demangled_count} demangled symbols")
endif()

set(_exported "")
set(_core_entities "")
foreach(_symbol _name IN ZIP_LISTS _mangled _demangled)
  # The Itanium ABI spells a namespace as its length and name, so a symbol without "9couchbase" in
  # its mangled name cannot belong to namespace couchbase.
  if(NOT _symbol MATCHES "9couchbase")
    continue()
  endif()
  entity_of(_entity "${_name}")
  set(_names_class FALSE)
  if(_name MATCHES "^(typeinfo name for|typeinfo for|vtable for|VTT for|construction vtable for) ")
    set(_names_class TRUE)
  endif()
  is_exported(_yes "${_entity}" ${_names_class})
  if(_yes)
    list(APPEND _exported "${_symbol}")
    if(_entity MATCHES "^couchbase::core::")
      list(APPEND _core_entities "${_entity}")
    endif()
  endif()
endforeach()
list(REMOVE_DUPLICATES _exported)
list(SORT _exported)

# A listed function the objects do not define is a typo or a rename, and the header calling it
# would fail to link.
foreach(_function IN LISTS COUCHBASE_PUBLIC_CORE_FUNCTIONS)
  if(NOT _function IN_LIST _core_entities)
    message(FATAL_ERROR "GenerateExportMap.cmake: ${_function} is listed in "
                        "COUCHBASE_PUBLIC_CORE_FUNCTIONS but no SDK object defines it")
  endif()
endforeach()

list(JOIN _exported ";\n    " _body)
file(WRITE "${OUTPUT}.tmp" "{\n  global:\n    ${_body};\n  local:\n    *;\n};\n")
execute_process(COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${OUTPUT}.tmp" "${OUTPUT}")
file(REMOVE "${OUTPUT}.tmp")
list(LENGTH _exported _n)
message(STATUS "${OUTPUT}: ${_n} exported symbols")
