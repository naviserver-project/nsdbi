#
# $Header$
#

#
# all.tcl --
#
#       This file contains a top-level script to run all of the tests.
#       Execute it by invoking "source all.tcl" when running nsd in
#       command mode in this directory.
#

package require Tcl 8.4
package require tcltest 2.2
namespace import tcltest::*
eval configure $argv -singleproc true -testdir [file dirname [info script]]


runAllTests
