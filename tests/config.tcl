#
# nsdbi test config
#


set homedir   [pwd]
set bindir    [file dirname [ns_info nsd]]



#
# Global server parameters.
#

ns_section "ns/parameters"
ns_param   home           $homedir
ns_param   tcllibrary     $bindir/../tcl
ns_param   logdebug       false

ns_section "ns/modules"
ns_param   nssock         $bindir/nssock.so
ns_param   global1        $homedir/nsdbitest.so
ns_param   global2        $homedir/nsdbitest.so

ns_section "ns/module/nssock"
ns_param   port            8080
ns_param   hostname        localhost
ns_param   address         127.0.0.1
ns_param   defaultserver   server1

ns_section "ns/module/nssock/servers"
ns_param   server1         server1

ns_section "ns/servers"
ns_param   server1         "Server One"


#
# Server One configuration.
#

ns_section "ns/server/server1/tcl"
ns_param   initfile        ${bindir}/init.tcl
ns_param   library         $homedir/tests/testserver/modules

ns_section "ns/server/server1/modules"
ns_param   db1           $homedir/nsdbitest.so
ns_param   db2           $homedir/nsdbitest.so
ns_param   OPENERR         $homedir/nsdbitest.so ;# nsdbitest will error on open
ns_param   OPENERR0        $homedir/nsdbitest.so

#
# Database configuration.
#

ns_section "ns/module/global1"
ns_param   maxhandles      2

ns_section "ns/module/global2"
ns_param   maxhandles      0

ns_section "ns/server/server1/module/db1"
ns_param   default         true
ns_param   maxhandles      5
ns_param   maxidle         20          ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen         40          ;# Handle closed after maxopen seconds, regardless of use.
ns_param   maxqueries      10000000       ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval   30          ;# Check for stale handles every 15 seconds.

ns_section "ns/server/server1/module/db2"
ns_param   maxhandles      1 ;# Set low for timeout test.

ns_section "ns/server/server1/module/OPENERR"
ns_param   maxhandles      1

ns_section "ns/server/server1/module/OPENERR0"
ns_param   maxhandles      0
