################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CC_SRCS += \
../src/rm/rm.cc \
../src/rm/rmtest_00.cc \
../src/rm/rmtest_01.cc \
../src/rm/rmtest_02.cc \
../src/rm/rmtest_03.cc \
../src/rm/rmtest_04.cc \
../src/rm/rmtest_05.cc \
../src/rm/rmtest_06.cc \
../src/rm/rmtest_07.cc \
../src/rm/rmtest_08.cc \
../src/rm/rmtest_09.cc \
../src/rm/rmtest_10.cc \
../src/rm/rmtest_11.cc \
../src/rm/rmtest_12.cc \
../src/rm/rmtest_13.cc \
../src/rm/rmtest_13b.cc \
../src/rm/rmtest_14.cc \
../src/rm/rmtest_15.cc \
../src/rm/rmtest_create_tables.cc \
../src/rm/rmtest_delete_tables.cc \
../src/rm/rmtest_extra_1.cc \
../src/rm/rmtest_extra_2.cc 

CC_DEPS += \
./src/rm/rm.d \
./src/rm/rmtest_00.d \
./src/rm/rmtest_01.d \
./src/rm/rmtest_02.d \
./src/rm/rmtest_03.d \
./src/rm/rmtest_04.d \
./src/rm/rmtest_05.d \
./src/rm/rmtest_06.d \
./src/rm/rmtest_07.d \
./src/rm/rmtest_08.d \
./src/rm/rmtest_09.d \
./src/rm/rmtest_10.d \
./src/rm/rmtest_11.d \
./src/rm/rmtest_12.d \
./src/rm/rmtest_13.d \
./src/rm/rmtest_13b.d \
./src/rm/rmtest_14.d \
./src/rm/rmtest_15.d \
./src/rm/rmtest_create_tables.d \
./src/rm/rmtest_delete_tables.d \
./src/rm/rmtest_extra_1.d \
./src/rm/rmtest_extra_2.d 

OBJS += \
./src/rm/rm.o \
./src/rm/rmtest_00.o \
./src/rm/rmtest_01.o \
./src/rm/rmtest_02.o \
./src/rm/rmtest_03.o \
./src/rm/rmtest_04.o \
./src/rm/rmtest_05.o \
./src/rm/rmtest_06.o \
./src/rm/rmtest_07.o \
./src/rm/rmtest_08.o \
./src/rm/rmtest_09.o \
./src/rm/rmtest_10.o \
./src/rm/rmtest_11.o \
./src/rm/rmtest_12.o \
./src/rm/rmtest_13.o \
./src/rm/rmtest_13b.o \
./src/rm/rmtest_14.o \
./src/rm/rmtest_15.o \
./src/rm/rmtest_create_tables.o \
./src/rm/rmtest_delete_tables.o \
./src/rm/rmtest_extra_1.o \
./src/rm/rmtest_extra_2.o 


# Each subdirectory must supply rules for building sources it contributes
src/rm/%.o: ../src/rm/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


