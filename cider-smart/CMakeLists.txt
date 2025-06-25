cmake_minimum_required(VERSION 3.10.2 FATAL_ERROR)
project(SMART)

# set (CMAKE_C_COMPILER "/usr/local/openmpi/bin/mpicxx")
# set (CMAKE_CXX_COMPILER ${CMAKE_C_COMPILER})

#Compiler options
# set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare -g")   # -DNDEBUG
set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wno-unused-variable -Wno-unused-but-set-variable -Wsign-compare -O3")  # -O3
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17")

#Link Options
set(LINKS_FLAGS "-lcityhash -lboost_system -lboost_coroutine -lpthread -libverbs -lmemcached -ltbb")

#Env Options
option (STATIC_MN_IP "Use static MNs according the IPs of the nodes" OFF)
option (ENABLE_CORO "Turn on the coroutine" OFF)
option (ENABLE_CACHE "Turn on the computing-side cache" ON)
option (LONG_TEST_EPOCH "Use big epoch num and long epoch duration" OFF)
option (SHORT_TEST_EPOCH "Use small epoch num and short epoch duration" OFF)
option (MIDDLE_TEST_EPOCH "Use middle epoch num and short epoch duration" OFF)

if(STATIC_MN_IP)
    add_definitions(-DSTATIC_ID_FROM_IP)
else()
    remove_definitions(-DSTATIC_ID_FROM_IP)
endif()

if(ENABLE_CORO)
    add_definitions(-DUSE_CORO)
else()
    remove_definitions(-DUSE_CORO)
endif()


if(ENABLE_CACHE)
    add_definitions(-DTREE_ENABLE_CACHE)
else()
    remove_definitions(-DTREE_ENABLE_CACHE)
endif()

if(LONG_TEST_EPOCH)
    add_definitions(-DLONG_TEST_EPOCH)
else()
    remove_definitions(-DLONG_TEST_EPOCH)
endif()

if(SHORT_TEST_EPOCH)
    add_definitions(-DSHORT_TEST_EPOCH)
else()
    remove_definitions(-DSHORT_TEST_EPOCH)
endif()

if(MIDDLE_TEST_EPOCH)
    add_definitions(-DMIDDLE_TEST_EPOCH)
else()
    remove_definitions(-DMIDDLE_TEST_EPOCH)
endif()

#Tree Options (compile into SMART/baselines; these options should be set up one after one; SMART is the ART that turns on all options)
option (ART_INDEXED_CACHE "+ ART-indexed Cache" ON)
option (HOMOGENEOUS_INTERNAL_NODE "+ Homogeneous adaptive internal node" ON)
option (LOCK_FREE_INTERNAL_NODE "+ Lock-free internal node" ON)
option (UPDATE_IN_PLACE_LEAF_NODE "+ Update-in-place leaf node" ON)
option (REAR_EMBEDDED_LOCK "+ Read embedded lock" ON)
option (READ_DELEGATION "+ Read delegation" ON)
option (WRITE_COMBINING "+ Write combining" ON)
if(MEMORY_NODE_NUM)
    add_definitions(-DMEMORY_NODE_NUM=${MEMORY_NODE_NUM})
endif()
if(HO_GROUP_SIZE)
    add_definitions(-DHO_GROUP_SIZE=${HO_GROUP_SIZE})
endif()
if(ST_LIM)
    add_definitions(-DST_LIM=${ST_LIM})
endif()
if(ST_SENTI)
    add_definitions(-DST_SENTI=${ST_SENTI})
endif()
if(SVSIZE)
    add_definitions(-DSVSIZE=${SVSIZE})
endif()
if(USE_MCS)
    add_definitions(-DUSE_MCS)
endif()
if(USE_MCS_WC)
    add_definitions(-DUSE_MCS_WC)
endif()
if(USE_MCS_WCV)
    add_definitions(-DUSE_MCS_WCV)
endif()
if(USE_FT)
    add_definitions(-DUSE_FT)
endif()
if(USE_FTERR)
    add_definitions(-DUSE_FTERR)
endif()
if(USE_ATOMIC)
    add_definitions(-DUSE_ATOMIC)
endif()
if(USE_ST)
    add_definitions(-DUSE_ST)
endif()
if(DELAY_WRITE)
    add_definitions(-DDELAY_WRITE)
endif()
#Naive ART
add_definitions(-DTREE_ENABLE_ART)
remove_definitions(-DTREE_ENABLE_FINE_GRAIN_NODE)
add_definitions(-DTREE_TEST_ROWEX_ART)
add_definitions(-DTREE_TEST_HOCL_HANDOVER)

remove_definitions(-DTREE_ENABLE_IN_PLACE_UPDATE)
remove_definitions(-DTREE_ENABLE_EMBEDDING_LOCK)

remove_definitions(-DTREE_ENABLE_READ_DELEGATION)
remove_definitions(-DTREE_ENABLE_WRITE_COMBINING)

##Cache
if(ART_INDEXED_CACHE)
    add_definitions(-DCACHE_ENABLE_ART)
else()
    remove_definitions(-DCACHE_ENABLE_ART)
endif()

##Tree
if(HOMOGENEOUS_INTERNAL_NODE)
    add_definitions(-DTREE_ENABLE_FINE_GRAIN_NODE)
else()
    remove_definitions(-DTREE_ENABLE_FINE_GRAIN_NODE)
endif()

if(LOCK_FREE_INTERNAL_NODE)
    remove_definitions(-DTREE_TEST_ROWEX_ART)
else()
    add_definitions(-DTREE_TEST_ROWEX_ART)
endif()

if(UPDATE_IN_PLACE_LEAF_NODE)
    add_definitions(-DTREE_ENABLE_IN_PLACE_UPDATE)
else()
    remove_definitions(-DTREE_ENABLE_IN_PLACE_UPDATE)
endif()

if(REAR_EMBEDDED_LOCK)
    add_definitions(-DTREE_ENABLE_EMBEDDING_LOCK)
else()
    remove_definitions(-DTREE_ENABLE_EMBEDDING_LOCK)
endif()

if(READ_DELEGATION)
    add_definitions(-DTREE_ENABLE_READ_DELEGATION)
else()
    remove_definitions(-DTREE_ENABLE_READ_DELEGATION)
endif()

if(WRITE_COMBINING)
    remove_definitions(-DTREE_TEST_HOCL_HANDOVER)
    add_definitions(-DTREE_ENABLE_WRITE_COMBINING)
else()
    add_definitions(-DTREE_TEST_HOCL_HANDOVER)
    remove_definitions(-DTREE_ENABLE_WRITE_COMBINING)
endif()

#Include files
set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})

#Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

#Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(SMART STATIC ${COMMON_FILE})
link_libraries(SMART)

#Test codes
file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
foreach (TEST ${TEST_SRC})
    get_filename_component(TEST_NAME ${TEST} NAME_WE)
    add_executable(${TEST_NAME} ${TEST})
    target_link_libraries(${TEST_NAME} ${LINKS_FLAGS})
endforeach()
