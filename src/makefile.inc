## For students: change this path to the root of your code if this doesn't work
CODEROOT = ..

#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
# with debugging info and the C++11 feature

CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11 -L/usr/lib/x86_64-linux-gnu -lboost_filesystem -lboost_system

