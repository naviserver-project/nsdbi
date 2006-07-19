#
# nsdbi configuration example.
#


set homedir   [pwd]
set bindir    [file dirname [ns_info nsd]]



#
# Global AOLserver parameters.
#

ns_section "ns/parameters"
ns_param   home           $homedir
ns_param   tcllibrary     $bindir/../tcl
ns_param   logdebug       false

ns_section "ns/modules"
ns_param   nssock         $bindir/nssock.so

ns_section "ns/module/nssock"
ns_param   port            8080
ns_param   hostname        localhost
ns_param   address         127.0.0.1
ns_param   defaultserver   server1

ns_section "ns/module/nssock/servers"
ns_param   server1            server1

ns_section "ns/servers"
ns_param   server1        "Server One"


#
# Server One configuration.
#

ns_section "ns/server/server1/tcl"
ns_param   initfile        ${bindir}/init.tcl
ns_param   library         $homedir/tests/testserver/modules

ns_section "ns/server/server1/modules"
ns_param   nsdbitest       $homedir/nsdbitest.so

#
# Database configuration.
#

ns_section "ns/server/server1/dbi"
ns_param   defaultpool     pool1

ns_section "ns/server/server1/module/nsdbitest/pools"
ns_param   pool1           "Pool One Description"
ns_param   pool2           "Pool Two Description"

ns_section "ns/server/server1/module/nsdbitest/pool/pool1"
ns_param   driver          dbitest
ns_param   handles         5
ns_param   verbose         off
ns_param   logsqlerrors    on
ns_param   datasource      "::datasource1"
ns_param   user            "username1"
ns_param   password        "password1"
ns_param   maxidle         20          ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen         40          ;# Handle closed after maxopen seconds, regardles of use.
ns_param   maxqueries      10          ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval   15          ;# Check for stale handles every 15 seconds.

ns_section "ns/server/server1/module/nsdbitest/pool/pool2"
ns_param   driver          dbitest
ns_param   handles         2
ns_param   datasource      "::datasource2"
ns_param   user            "username2"
ns_param   password        "password2"
