project(rime-predict)
cmake_minimum_required(VERSION 3.10)

aux_source_directory(src predict_src)
list(REMOVE_ITEM predict_src ${CMAKE_CURRENT_SOURCE_DIR}/src/predict_data_tool.cc)
list(REMOVE_ITEM predict_src src/predict_data_tool.cc)
list(REMOVE_ITEM predict_src predict_data_tool.cc)

# msgpack (保留用于旧数据迁移)
include_directories(deps/msgpack-c/include)

add_library(rime-predict-objs OBJECT ${predict_src})
if(BUILD_SHARED_LIBS)
  set_target_properties(rime-predict-objs
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
endif()

add_executable(predict_data_tool src/predict_data_tool.cc)
target_link_libraries(predict_data_tool PRIVATE ${rime_library} leveldb)

set(plugin_name rime-predict PARENT_SCOPE)
set(plugin_objs $<TARGET_OBJECTS:rime-predict-objs> PARENT_SCOPE)
set(plugin_deps ${rime_library} PARENT_SCOPE)
set(plugin_modules "predict" PARENT_SCOPE)
