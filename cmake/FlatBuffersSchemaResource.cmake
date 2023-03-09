
function(phantom_add_flatbuffers_schema_resources
    flatbuffersTarget
    resourceLibraryTarget
    whence
)
    set(all_binary_schema_files "")
    
    get_target_property(
        target_schemas
        ${flatbuffersTarget}
        INTERFACE_SOURCES)

    foreach(source ${target_schemas})
        get_filename_component(extension ${source} LAST_EXT)
        
        if(extension STREQUAL ".bfbs")
            list(APPEND all_binary_schema_files ${source})
        endif()
    endforeach()

    cmrc_add_resources(
        ${resourceLibraryTarget}
        WHENCE ${whence} 
        ${all_binary_schema_files}
    )
endfunction()
