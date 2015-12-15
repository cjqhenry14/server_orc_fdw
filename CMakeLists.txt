cmake_minimum_required(VERSION 3.3)
project(orc_fdw)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")


set(SOURCE_FILES
        orc_fdw--1.0.1.sql
        orc_fdw.c
        orc_fdw.control
        Makefile
        orcLib/
        orcInclude/)

include_directories(/Applications/Postgres.app/Contents/Versions/9.4//include/postgresql/server)


add_executable(orc_fdw ${SOURCE_FILES})