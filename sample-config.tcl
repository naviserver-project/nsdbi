--- sample-config.tcl
+++ sample-config.tcl
@@ -0,0 +1,0 @@
#
# nsdbi configuration example.
#
#     Unlike the nsdb module you may be used to, drivers are loaded
#     directly, once for each pool.
#
#     nsdbi drivers loaded globaly are available to all virtual
#     servers. Otherwise only the server that loaded the driver
#     can access that pool.
#
#     In this example, the nsdbipg Postgres module is being used.
#


#
# Global pools.
#
ns_section "ns/modules"
ns_param   pool1          $bindir/nsdbipg.so
ns_param   pool2          $bindir/nsdbipg.so


#
# Private pools
#
ns_section "ns/server/server1/modules"
ns_param   pool3          $bindir/nsdbipg.so
ns_param   pool4          $bindir/nsdbipg.so


#
# Pool 4 configuration.
#
ns_section "ns/server/server1/module/pool4"
ns_param   default        true ;# This is the default pool for server1.
ns_param   maxhandles     2    ;# Max open handles to db.
ns_param   timeout        10   ;# Seconds to wait if handle unavailable.
ns_param   maxrows        1000 ;# Default max rows a query may return.
ns_param   maxidle        0    ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen        0    ;# Handle closed after maxopen seconds, regardles of use.
ns_param   maxqueries     0    ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval  600  ;# Check for stale handles every 15 seconds.
#
# The following depend on which driver is being used, but you can
# expect user, password, database.
#
ns_param   user           test
ns_param   password       secret
ns_param   database       testdb

