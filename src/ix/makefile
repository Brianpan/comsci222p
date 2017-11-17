
include ../makefile.inc

#all: libix.a ixtest_01 ixtest_02 ixtest_03 ixtest_04 ixtest_05 ixtest_06 ixtest_07 ixtest_08 ixtest_09 ixtest_10 ixtest_11 ixtest_12 ixtest_13 ixtest_14 ixtest_15 ixtest_extra_01 ixtest_extra_02 
#all: libix.a ixtest_search ixtest_leafnode_search ixtest_insert_leafnode ixtest_split_leafnode ixtest_split_intermediatenode ixtest_fixedinsert01 ixtest_fixedinsert02 ixtest_varchar_leafnode_search
#all: libix.a ixtest_varchar_leafnode_search ixtest_varchar_intermediatenode ixtest_fixed06 ixtest_char
all: ixtest_p1 ixtest_p2 ixtest_p3 ixtest_p4 ixtest_p5 ixtest_p6 ixtest_pe_01 ixtest_pe_02
# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

ix_test_util.o: ix_test_util.h
ixtest_01.o: ix_test_util.h
ixtest_02.o: ix_test_util.h
ixtest_03.o: ix_test_util.h
ixtest_04.o: ix_test_util.h
ixtest_05.o: ix_test_util.h
ixtest_06.o: ix_test_util.h
ixtest_07.o: ix_test_util.h
ixtest_08.o: ix_test_util.h
ixtest_09.o: ix_test_util.h
ixtest_10.o: ix_test_util.h
ixtest_11.o: ix_test_util.h
ixtest_12.o: ix_test_util.h
ixtest_13.o: ix_test_util.h
ixtest_14.o: ix_test_util.h
ixtest_15.o: ix_test_util.h
ixtest_extra_01.o: ix_test_util.h
ixtest_extra_02.o: ix_test_util.h
ixtest_p1.o: ix_test_util.h
ixtest_p2.o: ix_test_util.h
ixtest_p3.o: ix_test_util.h
ixtest_p4.o: ix_test_util.h
ixtest_p5.o: ix_test_util.h
ixtest_p6.o: ix_test_util.h
ixtest_pe_01.o: ix_test_util.h
ixtest_pe_02.o: ix_test_util.h

# unit tests
ixtest_search.o: ix_test_util.h
ixtest_leafnode_search.o: ix_test_util.h
ixtest_insert_leafnode.o: ix_test_util.h
ixtest_split_leafnode.o: ix_test_util.h
ixtest_split_intermediatenode.o: ix_test_util.h
ixtest_fixedinsert01.o: ix_test_util.h
ixtest_fixedinsert02.o: ix_test_util.h
ixtest_varchar_leafnode_search.o: ix_test_util.h
ixtest_varchar_intermediatenode.o: ix_test_util.h
ixtest_fixed06.o: ix_test_util.h
ixtest_char.o: ix_test_util.h

# binary dependencies
ixtest_01: ixtest_01.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_02: ixtest_02.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_03: ixtest_03.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_04: ixtest_04.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_05: ixtest_05.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_06: ixtest_06.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_07: ixtest_07.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_08: ixtest_08.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_09: ixtest_09.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_10: ixtest_10.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_11: ixtest_11.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_12: ixtest_12.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_13: ixtest_13.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_14: ixtest_14.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_15: ixtest_15.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_01: ixtest_extra_01.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_02: ixtest_extra_02.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_p1: ixtest_p1.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_p2: ixtest_p2.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_p3: ixtest_p3.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_p4: ixtest_p4.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_p5: ixtest_p5.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_p6: ixtest_p6.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_pe_01: ixtest_pe_01.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_pe_02: ixtest_pe_02.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_varchar_leafnode_search: ixtest_varchar_leafnode_search.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_varchar_intermediatenode: ixtest_varchar_intermediatenode.o libix.a $(CODEROOT)/rbf/librbf.a

# unit tests
ixtest_search: ixtest_search.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_leafnode_search: ixtest_leafnode_search.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_insert_leafnode: ixtest_insert_leafnode.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_split_leafnode: ixtest_split_leafnode.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_split_intermediatenode: ixtest_split_intermediatenode.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_fixedinsert01: ixtest_fixedinsert01.o  libix.a $(CODEROOT)/rbf/librbf.a
ixtest_fixedinsert02: ixtest_fixedinsert02.o  libix.a $(CODEROOT)/rbf/librbf.a
# for varchar
ixtest_varchar_leafnode_search: ixtest_varchar_leafnode_search.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_varchar_intermediatenode: ixtest_varchar_intermediatenode.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_fixed06: ixtest_fixed06.o libix.a $(CODEROOT)/rbf/librbf.a
ixtest_char: ixtest_char libix.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm *.o *.a ixtest_01 ixtest_02 ixtest_03 ixtest_04 ixtest_05 ixtest_06 ixtest_07 ixtest_08 ixtest_09 ixtest_10 ixtest_11 ixtest_12 ixtest_13 ixtest_14 ixtest_15 ixtest_extra_01 ixtest_extra_02 ixtest_p1 ixtest_p2 ixtest_p3 ixtest_p4 ixtest_p5 ixtest_p6 ixtest_pe_01 ixtest_pe_02 
#	-rm *.o *.a ixtest_search ixtest_leafnode_search ixtest_insert_leafnode ixtest_split_leafnode ixtest_split_intermediatenode ixtest_fixedinsert01 ixtest_fixedinsert02 ixtest_varchar_leafnode_search ixtest_varchar_intermediatenode ixtest_fixed06 ixtest_char
	$(MAKE) -C $(CODEROOT)/rbf clean
