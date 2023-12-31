#
# The contents of this file are subject to the AOLserver Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://aolserver.com/.
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is AOLserver Code and related documentation
# distributed by AOL.
# 
# The Initial Developer of the Original Code is America Online,
# Inc. Portions created by AOL are Copyright (C) 1999 America Online,
# Inc. All Rights Reserved.
#
# Copyright (C) 2004-2008 Stephen Deasey <sdeasey@gmail.com>
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License (the "GPL"), in which case the
# provisions of GPL are applicable instead of those above.  If you wish
# to allow use of your version of this file only under the terms of the
# GPL and not to allow others to use your version of this file under the
# License, indicate your decision by deleting the provisions above and
# replace them with the notice and other provisions required by the GPL.
# If you do not delete the provisions above, a recipient may use your
# version of this file under either the License or the GPL.
# 
#

VERSION     = 0.4

ifndef NAVISERVER
    NAVISERVER  = /usr/local/ns
endif

NSD         = $(NAVISERVER)/bin/nsd

MODNAME     = nsdbi

LIBNM       = nsdbi
LIBOBJS     = init.o tclcmds.o tclsubst.o
LIBHDRS     = nsdbi.h nsdbidrv.h

MOD         = nsdbitest.so
MODOBJS     = nsdbitest.o

TCL         = util.tcl

include $(NAVISERVER)/include/Makefile.module


DTPLITE = dtplite

doc: html-doc man-doc

html-doc:
	$(MKDIR) doc/html
	$(DTPLITE) -o doc/html html doc/src/mann/nsdbi.man
	$(DTPLITE) -o doc/html html doc/src/man3/*.man

man-doc:
	$(MKDIR) doc/man
	$(DTPLITE) -o doc/man nroff doc/src/mann/nsdbi.man
	$(DTPLITE) -o doc/man nroff doc/src/man3/*.man



NS_TEST_CFG	= -c -d -t tests/config.tcl
NS_TEST_ALL	= tests/all.tcl $(TCLTESTARGS)
LD_LIBRARY_PATH	= LD_LIBRARY_PATH="./:$$LD_LIBRARY_PATH"

test: all
	export $(LD_LIBRARY_PATH); $(NSD) $(NS_TEST_CFG) $(NS_TEST_ALL)

runtest: all
	export $(LD_LIBRARY_PATH); $(NSD) $(NS_TEST_CFG)

gdbtest: all
	@echo set args $(NS_TEST_CFG) $(NS_TEST_ALL) > gdb.run
	export $(LD_LIBRARY_PATH); gdb -x gdb.run $(NSD)
	rm gdb.run

gdbruntest: all
	@echo set args $(NS_TEST_CFG) > gdb.run
	export $(LD_LIBRARY_PATH); gdb -x gdb.run $(NSD)
	rm gdb.run

memcheck: all
	export $(LD_LIBRARY_PATH); valgrind --tool=memcheck $(NSD) $(NS_TEST_CFG) $(NS_TEST_ALL)

runmemcheck: all
	export $(LD_LIBRARY_PATH); valgrind --tool=memcheck $(NSD) $(NS_TEST_CFG)


SRCS = init.c tclcmds.c tclsubst.c nsdbitest.c util.tcl $(LIBHDRS)
EXTRA = README NEWS TODO license.terms sample-config.tcl version_include.man \
		Makefile doc tests

dist: doc all
	rm -rf $(MODNAME)-$(VERSION)
	mkdir $(MODNAME)-$(VERSION)
	$(CP) $(SRCS) $(EXTRA) $(MODNAME)-$(VERSION)
	hg log > $(MODNAME)-$(VERSION)/ChangeLog
	tar czf $(MODNAME)-$(VERSION).tgz $(MODNAME)-$(VERSION)


.PHONY: doc html-doc man-doc
