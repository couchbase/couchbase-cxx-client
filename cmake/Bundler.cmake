# Based on the article from Cristian Adam
#
# Source: https://cristianadam.eu/20190501/bundling-together-static-libraries-with-cmake/
#
# Archived: https://archive.is/OcuwT

function(bundle_static_library tgt_name bundled_tgt_name)
  list(APPEND static_libs ${tgt_name})

  function(strip_build_interface input output)
    string(
      REGEX MATCH
            "\\$<BUILD_INTERFACE:([^>]+)>"
            _match
            "${input}")
    if(_match)
      string(
        REGEX
        REPLACE "\\$<BUILD_INTERFACE:([^>]+)>"
                "\\1"
                stripped
                "${input}")
      set(${output}
          "${stripped}"
          PARENT_SCOPE)
    else()
      set(${output}
          "${input}"
          PARENT_SCOPE)
    endif()
  endfunction()

  function(_recursively_collect_dependencies input_target)
    # Both properties, not one. A concrete target's LINK_LIBRARIES holds what it links for itself,
    # while INTERFACE_LINK_LIBRARIES holds what it imposes on whoever links IT -- and the second is
    # exactly what a consumer of the installed archive inherits. cmake/DetectStandardFilesystem.cmake
    # adds stdc++fs that way, so reading only the first would drop a library that is mandatory on
    # older toolchains. An INTERFACE_LIBRARY has no LINK_LIBRARIES at all; get_target_property simply
    # reports it absent and the loop below skips the sentinel.
    set(public_dependencies)
    get_target_property(_input_type ${input_target} TYPE)
    if(NOT ${_input_type} STREQUAL "INTERFACE_LIBRARY")
      get_target_property(_own_dependencies ${input_target} LINK_LIBRARIES)
      if(_own_dependencies)
        list(APPEND public_dependencies ${_own_dependencies})
      endif()
    endif()
    get_target_property(_interface_dependencies ${input_target} INTERFACE_LINK_LIBRARIES)
    if(_interface_dependencies)
      list(APPEND public_dependencies ${_interface_dependencies})
    endif()
    foreach(candidate IN LISTS public_dependencies)
      strip_build_interface("${candidate}" dependency)
      if(TARGET ${dependency})
        get_target_property(alias ${dependency} ALIASED_TARGET)
        if(TARGET ${alias})
          set(dependency ${alias})
        endif()
        get_target_property(_type ${dependency} TYPE)
        if(${_type} STREQUAL "STATIC_LIBRARY")
          list(APPEND static_libs ${dependency})
        else()
          get_target_property(_imported ${dependency} IMPORTED)
          if(_type MATCHES "^(SHARED|MODULE|UNKNOWN)_LIBRARY$"
             OR (_type STREQUAL "INTERFACE_LIBRARY" AND _imported))
            # Its content stays outside the archive, so a consumer of the installed archive has to
            # be told about it. Recorded, not archived.
            #
            # An IMPORTED interface library counts even though it contributes no objects: that is
            # the shape pkg_check_modules(... IMPORTED_TARGET) produces, and dropping it would lose
            # the link directories and options that come with it -- which is the whole reason
            # cmake/OpenSSL.cmake falls back to pkg-config in the first place. An interface target
            # defined in this build is a different thing: it carries headers, and whatever it links
            # is reached by recursing through it.
            #
            # OBJECT libraries are never recorded: their objects are compiled straight into the
            # target being archived.
            list(APPEND external_libs ${dependency})
          endif()
        endif()

        # Recurse into targets this build defines, and stop at imported ones.
        #
        # An imported target describes something already built elsewhere: whatever it needs comes
        # back with it when the consumer calls find_dependency on the package that defines it, so
        # naming its interface piecemeal adds nothing. Descending into one is actively harmful --
        # the platform's gRPC drags in around a hundred and seventy absl:: interface targets, none
        # of which a consumer has to name because linking gRPC::grpc++ supplies them all.
        # An imported STATIC library is the exception: it gets archived like any other, so whatever
        # it needs is now inside the bundle and its interface still has to be walked. Only imported
        # targets that stay OUTSIDE the archive are left unexplored.
        get_target_property(_dependency_imported ${dependency} IMPORTED)
        if(_type STREQUAL "STATIC_LIBRARY")
          set(_dependency_imported FALSE)
        endif()
        get_property(library_already_added GLOBAL PROPERTY _${tgt_name}_static_bundle_${dependency})
        if(NOT library_already_added AND NOT _dependency_imported)
          set_property(GLOBAL PROPERTY _${tgt_name}_static_bundle_${dependency} ON)
          _recursively_collect_dependencies(${dependency})
        endif()
      elseif(dependency
             AND NOT dependency MATCHES "NOTFOUND$"
             AND NOT dependency MATCHES "^\\$<"
             AND NOT dependency MATCHES "^-[^l]")
        # A plain link item rather than a target -- bcrypt.lib, iphlpapi, m, rt, or an absolute path
        # from find_library. These are exactly the dependencies that were repeatedly missed when the
        # installed interface was maintained by hand, because they are contributed from wherever the
        # subdirectory that needs them happens to be.
        #
        # Two things are not link items and are skipped: a generator expression, which cannot be
        # evaluated here, and the "<var>-NOTFOUND" sentinel get_target_property returns for a target
        # that has no link property at all, which would otherwise be published verbatim as
        # -lpublic_dependencies-NOTFOUND.
        list(APPEND external_libs ${dependency})
      elseif(dependency MATCHES "^-[^l]")
        # A link OPTION rather than a library -- enable_sanitizers() adds -fsanitize=... PUBLIC, and
        # an archive built with it carries references into the sanitizer runtime that a consumer's
        # link line must repeat. Kept apart from the libraries because the two are published
        # differently: options as INTERFACE_LINK_OPTIONS and raw entries in Libs.private, libraries
        # as -l names.
        list(APPEND external_opts ${dependency})
      endif()
    endforeach()
    set(static_libs
        ${static_libs}
        PARENT_SCOPE)
    set(external_libs
        ${external_libs}
        PARENT_SCOPE)
    set(external_opts
        ${external_opts}
        PARENT_SCOPE)
  endfunction()

  _recursively_collect_dependencies(${tgt_name})

  list(REMOVE_DUPLICATES static_libs)
  if(external_libs)
    list(REMOVE_DUPLICATES external_libs)
  endif()
  if(external_opts)
    list(REMOVE_DUPLICATES external_opts)
  endif()
  # Published for cmake/Packaging.cmake, which turns it into the installed target's link interface
  # and its pkg-config Libs.private, and warns about anything it does not already name.
  set_property(GLOBAL PROPERTY ${bundled_tgt_name}_EXTERNAL_LIBS "${external_libs}")
  set_property(GLOBAL PROPERTY ${bundled_tgt_name}_EXTERNAL_OPTS "${external_opts}")

  set(bundled_tgt_full_name
      ${CMAKE_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${bundled_tgt_name}${CMAKE_STATIC_LIBRARY_SUFFIX})

  # Selected on the platform before the compiler, because what matters here is which ar is on PATH,
  # not who compiled the objects. Apple's ar comes from cctools and has no MRI mode -- no -M, no
  # ADDLIB -- so on macOS the archives are unpacked and re-archived instead. Keying that branch on
  # AppleClang alone left the MRI path selected for a macOS build driven by any other Clang, a
  # Homebrew or MacPorts LLVM among them, where it cannot work.
  if(APPLE)
    find_program(find NAMES find REQUIRED)

    set(AR_EXTRACT_DIR "${CMAKE_CURRENT_BINARY_DIR}/tmp_ar_extract")
    set(EXTRACT_COMMANDS "")
    foreach(tgt IN LISTS static_libs)
      list(
        APPEND EXTRACT_COMMANDS
        COMMAND ${CMAKE_COMMAND} -E make_directory ${AR_EXTRACT_DIR}/${tgt}
        COMMAND ${CMAKE_COMMAND} -E chdir ${AR_EXTRACT_DIR}/${tgt} ${CMAKE_AR} -x $<TARGET_FILE:${tgt}>)
    endforeach()

    add_custom_command(
      OUTPUT ${bundled_tgt_full_name}
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${AR_EXTRACT_DIR}
      COMMAND ${CMAKE_COMMAND} -E make_directory ${AR_EXTRACT_DIR} ${EXTRACT_COMMANDS}
      COMMAND ${find} ${AR_EXTRACT_DIR} -name *.o -exec ${CMAKE_AR} -rcs ${bundled_tgt_full_name} {} +
      COMMAND ${CMAKE_AR} -s ${bundled_tgt_full_name}
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${AR_EXTRACT_DIR}
      DEPENDS ${tgt_name} ${static_libs}
      COMMENT "Bundling ${bundled_tgt_name}"
      VERBATIM)
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|GNU)$")
    file(WRITE ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in "CREATE ${bundled_tgt_full_name}\n")

    # GNU ar's MRI lexer does not accept '+' in a filename. binutils 2.35 (EL9) reads
    # "ADDLIB .../libgrpc++.a" as the file ".../libgrpc" followed by the stray token "++" and fails
    # with "libgrpc: No such file or directory"; 2.47 accepts it, so this only shows up on older
    # toolchains. gRPC contributes libgrpc++.a and several siblings, so any archive whose name
    # carries a '+' is staged under a '+'-free name just before ar runs. MRI needs nothing of that
    # path but the name, so how it gets there does not matter -- except on Windows, which this
    # branch also covers through MinGW, and where creating a symlink needs a privilege an ordinary
    # account does not hold. Copy there and link everywhere else.
    set(_bundle_staging ${CMAKE_BINARY_DIR}/bundle_staging)
    set(_bundle_stage_commands COMMAND ${CMAKE_COMMAND} -E make_directory ${_bundle_staging})
    if(WIN32)
      set(_bundle_stage_verb copy)
    else()
      set(_bundle_stage_verb create_symlink)
    endif()

    foreach(tgt IN LISTS static_libs)
      if(tgt MATCHES "\\+")
        string(REPLACE "+" "_plus" _bundle_safe_name "${tgt}")
        set(_bundle_staged_lib "${_bundle_staging}/lib${_bundle_safe_name}.a")
        list(APPEND _bundle_stage_commands COMMAND ${CMAKE_COMMAND} -E ${_bundle_stage_verb}
             $<TARGET_FILE:${tgt}> ${_bundle_staged_lib})
        file(APPEND ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in "ADDLIB ${_bundle_staged_lib}\n")
      else()
        file(APPEND ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in "ADDLIB $<TARGET_FILE:${tgt}>\n")
      endif()
    endforeach()

    file(APPEND ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in "SAVE\n")
    file(APPEND ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in "END\n")

    file(
      GENERATE
      OUTPUT ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar
      INPUT ${CMAKE_BINARY_DIR}/${bundled_tgt_name}.ar.in)

    set(ar_tool ${CMAKE_AR})
    if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
      set(ar_tool ${CMAKE_CXX_COMPILER_AR})
    endif()

    # GNU sed: -i takes no argument here. macOS never reaches this branch, so the BSD sed that
    # needed gsed is out of scope.
    find_program(SED sed REQUIRED)

    add_custom_command(
      ${_bundle_stage_commands}
      COMMAND ${SED} -i "s|${CMAKE_BINARY_DIR}/||g" ${bundled_tgt_name}.ar
      COMMAND ${ar_tool} -M < ${bundled_tgt_name}.ar
      OUTPUT ${bundled_tgt_full_name}
      DEPENDS ${tgt_name} ${static_libs}
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
      COMMENT "Bundling ${bundled_tgt_name}"
      VERBATIM)
  elseif(MSVC)
    get_filename_component(LINKER_DIR "${CMAKE_LINKER}" DIRECTORY)
    find_program(
      lib_tool
      NAMES lib
      HINTS "${LINKER_DIR}" REQUIRED)

    foreach(tgt IN LISTS static_libs)
      list(APPEND static_libs_full_names $<TARGET_FILE:${tgt}>)
    endforeach()

    add_custom_command(
      COMMAND ${lib_tool} /NOLOGO /OUT:${bundled_tgt_full_name} ${static_libs_full_names}
      OUTPUT ${bundled_tgt_full_name}
      DEPENDS ${tgt_name} ${static_libs}
      COMMENT "Bundling ${bundled_tgt_name}"
      VERBATIM)
  else()
    message(FATAL_ERROR "Unknown bundle scenario: CMAKE_CXX_COMPILER_ID=${CMAKE_CXX_COMPILER_ID}, MSVC=${MSVC}!")
  endif()

  add_custom_target(bundling_target ALL DEPENDS ${bundled_tgt_full_name})
  add_dependencies(bundling_target ${tgt_name})

  add_library(${bundled_tgt_name} STATIC IMPORTED)
  set_target_properties(
    ${bundled_tgt_name}
    PROPERTIES IMPORTED_LOCATION ${bundled_tgt_full_name} INTERFACE_INCLUDE_DIRECTORIES
                                                          $<TARGET_PROPERTY:${tgt_name},INTERFACE_INCLUDE_DIRECTORIES>)
  add_dependencies(${bundled_tgt_name} bundling_target)
endfunction()
