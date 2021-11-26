add_subdirectory(utils)

integration_test(management)
integration_test(crud)
integration_test(query)
integration_test(diagnostics)
integration_test(binary_operations)
integration_test(durability)
integration_test(arithmetic)
integration_test(collections)
integration_test(subdoc)

unit_test(connection_string)
unit_test(utils)

integration_benchmark(get)
