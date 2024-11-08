#
# nsdbi configuration example.
#
#     Unlike the old nsdb module you may be used to, drivers are loaded
#     directly, once for each pool.
#
#     nsdbi drivers loaded globally are available to all virtual
#     servers. Otherwise only the server that loaded the driver
#     can access that pool.
#
#     In this example, the nsdbipg Postgres module is being used.
#

#
# Global dbs.
#
ns_section "ns/modules"
ns_param   db1            nsdbipg.so
ns_param   db2            nsdbipg.so


#
# Per-server dbs
#
ns_section "ns/server/server1/modules"
ns_param   db3            nsdbipg.so
ns_param   db4            nsdbipg.so


#
# Example configuration for db4, other dbs will be similar.
#
ns_section "ns/server/server1/module/db4"
ns_param   default        true ;# This is the default pool for server1.
ns_param   maxhandles     0    ;# Max open handles to db (0 = per-thread).
ns_param   timeout        10   ;# Seconds to wait if handle unavailable.
ns_param   maxrows        1000 ;# Default max rows a query may return.
ns_param   maxidle        0    ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen        0    ;# Handle closed after maxopen seconds, regardless of use.
ns_param   maxqueries     0    ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval  600  ;# Check for stale handles every 10 minutes.
#
# The following depend on which driver is being used, but you can
# expect user, password, database.
#
ns_param   user           test
ns_param   password       secret
ns_param   database       testdb
