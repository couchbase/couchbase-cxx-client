add_subdirectory(utils)

integration_test(management)
integration_test(trivial_crud)
integration_test(trivial_query)
integration_test(diagnostics)
integration_test(binary_operations)
integration_test(durability)
integration_test(arithmetic)

unit_test(connection_string)
unit_test(utils)

integration_benchmark(get)
