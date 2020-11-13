function(phantom_protobuf_generate OUTPUTS)
    set(options)
    set(oneValueArgs "LANGUAGE")
    set(multiValueArgs "PROTO" "IMPORTDIRECTORY" "GENERATEDEXTENSIONS" "EXTRAARGS")
    cmake_parse_arguments("phantom_protobuf_generate" "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    
    message("phantom_protobuf_generate: ${ARGN}")
    set(_protobuf_include_path_args)
    set(_protobuf_include_path)
    set(_output_directory ${CMAKE_CURRENT_BINARY_DIR})

    set(_import_directories ${phantom_protobuf_generate_IMPORTDIRECTORY})
    message("phantom_protobuf_generate: _import_directories ${_import_directories}")
    foreach(_dir ${phantom_protobuf_generate_IMPORTDIRECTORY})
        get_filename_component(_abs ${_dir} ABSOLUTE)
        list(APPEND _protobuf_include_path_args -I ${_abs})
        list(APPEND _protobuf_include_path ${_abs})
    endforeach()

    if ("${phantom_protobuf_generate_LANGUAGE}" STREQUAL "cpp")
    else()
        message(SEND_ERROR "LANGUAGE must be one of (cpp)")
    endif()

    set(_outputs)

    foreach(_proto ${phantom_protobuf_generate_PROTO})
        set(_generated_sources)
        get_filename_component(_directory ${_proto} DIRECTORY)
        get_filename_component(_name_wle ${_proto} NAME_WLE)
        get_filename_component(_abs ${_proto} ABSOLUTE)

        set(_proto_src ${_abs})
        set(_proto_dependency ${_abs})
        foreach(_possible_path ${_protobuf_include_path})
            if(EXISTS "${_possible_path}/${_proto}")
                set(_proto_dependency "${_possible_path}/${_proto}")
            endif()
        endforeach()

        find_file(_relative ${_proto} ${_protobuf_include_path} NO_DEFAULT_PATH)

        foreach(_extension ${phantom_protobuf_generate_GENERATEDEXTENSIONS})
            list(APPEND _generated_sources "${_output_directory}/${_directory}/${_name_wle}${_extension}")
        endforeach()
        list(APPEND _outputs ${_generated_sources})

        message("Adding .proto file ${_proto}, generates ${_generated_sources}")

        add_custom_command(
            OUTPUT ${_generated_sources}
            COMMAND  protobuf::protoc
            ARGS --${phantom_protobuf_generate_LANGUAGE}_out=${_output_directory} ${_protobuf_include_path_args} ${_proto} ${phantom_protobuf_generate_EXTRAARGS}
            DEPENDS "${_proto_dependency}" protobuf::protoc
            COMMENT "Running ${phantom_protobuf_generate_LANGUAGE} protocol buffer compiler on ${_proto} to generate ${_generated_sources}"
            VERBATIM )

    endforeach()

    set_source_files_properties(
        ${_outputs}
        PROPERTIES
        GENERATED TRUE
    )

    set(${OUTPUTS} ${_outputs} PARENT_SCOPE)
endfunction()

function(phantom_grpc_generate_cpp SOURCES HEADERS)

    set(_generated_sources)

    phantom_protobuf_generate(
        _generated_sources
        LANGUAGE cpp
        GENERATEDEXTENSIONS ".grpc.pb.cc" ".grpc.pb.h"
        EXTRAARGS "--plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>" "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
        ${ARGN}
    )

    set(_sources)
    set(_headers)

    foreach(_generated_source ${_generated_sources})
        get_filename_component(_extension ${_generated_source} LAST_EXT)
        if ("${_extension}" STREQUAL ".h")
            list(APPEND _headers ${_generated_source})
        elseif("${_extension}" STREQUAL ".cc")
            list(APPEND _sources ${_generated_source})
        endif()
    endforeach()

    set(${SOURCES} ${_sources} PARENT_SCOPE)
    set(${HEADERS} ${_headers} PARENT_SCOPE)
endfunction()

function(phantom_protobuf_generate_cpp SOURCES HEADERS)

    set(_generated_sources)

    phantom_protobuf_generate(
        _generated_sources
        LANGUAGE cpp
        GENERATEDEXTENSIONS ".pb.cc" ".pb.h"
        ${ARGN}
    )

    set(_sources)
    set(_headers)

    foreach(_generated_source ${_generated_sources})
        get_filename_component(_extension ${_generated_source} LAST_EXT)
        if ("${_extension}" STREQUAL ".h")
            list(APPEND _headers ${_generated_source})
        elseif("${_extension}" STREQUAL ".cc")
            list(APPEND _sources ${_generated_source})
        endif()
    endforeach()

    set(${SOURCES} ${_sources} PARENT_SCOPE)
    set(${HEADERS} ${_headers} PARENT_SCOPE)
endfunction()
