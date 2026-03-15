project(rime-predict)
cmake_minimum_required(VERSION 3.10)

aux_source_directory(src predict_src)
include_directories(deps/msgpack-c/include)
include_directories(${RIME_SOURCE_DIR}/deps/leveldb/include)

add_library(rime-predict-objs OBJECT ${predict_src})
if(BUILD_SHARED_LIBS)
  set_target_properties(rime-predict-objs
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
endif()

set(plugin_name rime-predict PARENT_SCOPE)
set(plugin_objs $<TARGET_OBJECTS:rime-predict-objs> PARENT_SCOPE)
set(plugin_deps ${rime_library} leveldb PARENT_SCOPE)
set(plugin_modules "predict" PARENT_SCOPE)

