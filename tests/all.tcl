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


rename tcltest::test tcltest::__test

proc tcltest::test args {

    ns_log debug >>>>> \
        [format "%-16s" "[lindex $args 0]:"] ([lindex $args 1])

    uplevel 1 tcltest::__test $args

    ns_log debug <<<<< [lindex $args 0]
}

runAllTests
