DB_Micro: Minimalistic DB abstraction layer with replication support
Version: 2.x
(C) Dmitry Koterov, http://en.dklab.ru/lib/DB_Micro/
License: LGPL


ABSTRACT
--------

DB_Micro is an abstraction layer for relational databases (PostgreSQL driver
is available now only) with transparent asynchronous master-slave replication,
health checks and reach logging support.

Why yet another library for that? Because there are no good DB libraries which
could seamlessly work with master-slave replication. Even stand-alone solutions
(like pgpool-II for PostgreSQL) have insufficient slave lag detection mechanisms.
Can you believe that? I couldn't too, but after hours of searching I've found
nothing good enough for working in asynchronous replication environment
transparently.


WHAT IS A GOOD MASTER-SLAVE SUPPORTING LIBRARY
----------------------------------------------

When we work with a master-slave asynchronous replication, we always expect that
slaves are delayed relative to the master. So if a user writes something to the
master database, he should not read from a slave anymore, but use only the master -
at least until the slave has already applied the changes from the master made by
that user. Else, after the page is reloaded, user will not see his own changes on
it (for example).

The meaning of the phrase "the slave is up-to-date relative to the master" is not
system-wide, it is highly per-user based. Even in low-loaded systems slaves are
never "up to date", because there are more and more writes arriving all the time.
So there is no sense to wait until the slave is "up to date" - it never happens.

But for a particular user's session one could say that "the slave is up-to-date
already" after the user has written his changes. DB_Micro supports exactly that:
it binds the "master position" after each write to the user's session ID. So when
the user wants to read something from the slave a bit later, the slave's "position"
is compared to the saved master's position after the last write operation: if it's
less, reading from the slave is not allowed, and all user's queries go to the master.
All these is done automatically and transparently for the calling code.

So the main property of a good tool is the following: does it require the user's
session ID to be passed into the tool? If not, it does not support asynchronous
replication transparently enough.


SYNOPSIS
--------

// Create the engine. Note that the library detects who's the master automatically,
// it allows to make failover easily with no reconfiguration. Note that not user's
// SESSION_ID is passed, but the while user's session, this is handled by
// DB_Micro_Replication_StoragePos_Session.
$impl = new DB_Micro_Replication_Impl_Pgsql();
$storagePos = DB_Micro_Replication_StoragePos_Session();       // or create your own
$storageHealth = DB_Micro_Replication_StorageHealth_TmpFile(); // or create your own
$db = new DB_Micro_Replication(
    "pgsql://user:pwd@host1,host2,host3/dbname?connect_timeout=2&num_conn_tries=2&fail_check_interval=60",
    function($logMsg, $queryTime, $connName) { echo "You may use your own logger, of course."; },
    $impl,
    $storagePos,
    $storageHealth
);

// Read-only query (goes to a slave is it's up-to-date enough for the current session ID).
print_r($db->query("SELECT * FROM tbl WHERE a=? AND b=?", array("a", "b")));

// Read-write query (goes to the master).
$db->update("UPDATE tbl SET a=?", array("a"));

// Transactions (always start at the master and switch later queries to the master).
$db->beginTransaction();
$db->query("SELECT * FROM tbl"); // goes to the master, not to the slave
$db->commit();  // or $db->rollBack() to go back to the slave
