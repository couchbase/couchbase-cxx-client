add_library(system_metrics_cxx OBJECT
             src/os/linux_system_metrics.cxx
             src/os/macos_system_metrics.cxx
             src/os/windows_system_metrics.cxx
             src/system_metrics.cxx)

set_target_properties(system_metrics_cxx PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_project_warnings(system_metrics_cxx)
set_project_options(system_metrics_cxx)

if(WIN32)
  target_link_libraries(system_metrics_cxx PUBLIC psapi.lib)
endif()

target_include_directories(system_metrics_cxx
                            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
                            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
                                    ${CMAKE_CURRENT_SOURCE_DIR})
