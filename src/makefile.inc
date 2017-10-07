## For students: change this path to the root of your code if this doesn't work
CODEROOT = ..

#CC = gcc
CC = g++
LD_LIBRARY_PATH = LD_LIBRARY_PATH=$(CODEROOT)/boost/lib
#CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
# with debugging info and the C++11 feature

# -I means find header
# -L means find built sources dir
# -l means library of built source
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11

