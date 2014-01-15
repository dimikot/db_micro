<?php
class DB_Micro_Replication implements DB_Micro_IConnection
{
    const LOG_PREFIX = "Replication: ";
    const DSN_MASTER_SUFFIX = "#MASTER";

    /**
     * @var string
     */
    private $_sid;

    /**
     * @var callback
     */
    private $_logger;

    /**
     * @var DB_Micro_Replication_Impl_Abstract
     */
    private $_impl;

    /**
     * @var DB_Micro_Replication_KeyValueStorage_Abstract
     */
    private $_kvStorage;

    /**
     * @var DB_Micro_IConnection
     */
    private $_slaveConnCache = null;

    /**
     * @var DB_Micro_IConnection
     */
    private $_masterConnCache = null;

    /**
     * @var bool
     */
    private $_inTransaction = false;

    /**
     * @var bool
     */
    private $_hadUpdates = false;

    /**
     * @var bool
     */
    private $_hadUpatesBeforeTransaction = false;

    /**
     * @var mixed
     */
    private $_lastCommitPosCache = false;

    /**
     * @var array
     */
    private $_connStatesCache = array();

    /**
     * @var string
     */
    private $_dsn;

    /**
     * @var string
     */
    private $_dsnWithoutHost;

    /**
     * @var array
     */
    private $_dsnHosts = array();

    /**
     * @var int
     */
    private $_paramFailCheckInterval = 600;

    /**
     * @var bool
     */
    private $_paramMasterAsReplica = false;

    /**
     * DSN format is:
     * proto://[user:pass@]host1[:port1][,host2[:port2]][,...]/dbname?[params]
     *
     * Parameters may include:
     * - fail_check_interval=<seconds>: how often we should try to revive dead hosts
     * - master_as_replica=<1|0>: if 1, do not search for a slave if we've already found a master
     *
     * @param string $dsn
     * @param callback $logger
     * @param string $sid      If null, no master pos is saved and traced between connections.
     * @param DB_Micro_Replication_Impl_Abstract $impl
     * @param DB_Micro_Replication_KeyValueStorage_Abstract $kvStorage
     * @throws DB_Micro_Exception
     */
    public function __construct($dsn, $logger, $sid, $impl, $kvStorage)
    {
        $this->_sid = $sid;
        $this->_logger = $logger;
        $this->_impl = $impl;
        $this->_kvStorage = $kvStorage;
        $this->_dsn = $dsn;
        if (preg_match('{^ (\w+:// (?:[^@]+@)?) ([^/]+) ( /[^?]* (?:\?(.*))? ) $}xs', $this->_dsn, $m)) {
            $this->_dsnWithoutHost = $m[1] . "*" . $m[3];
            foreach (preg_split('/,/s', $m[2]) as $host) {
                $host = trim($host);
                if ($host) $this->_dsnHosts[] = $host;
            }
            if (!empty($m[4])) {
                $params = array();
                parse_str($m[4], $params);
                if (isset($params['fail_check_interval'])) {
                    $this->_paramFailCheckInterval = $params['fail_check_interval'];
                }
                if (isset($params['master_as_replica'])) {
                    $this->_paramMasterAsReplica = $params['master_as_replica'];
                }
            }
        } else {
            throw new DB_Micro_Exception("Cannot parse the passed DSN.", 'connect');
        }
    }

    /**
     * This is typically called at the beginning of a background daemon script
     * to force using of the master DB only. The method also closes the slave
     * connection and remains only one - to the master.
     *
     * @return self
     */
    public function switchToMaster()
    {
        $this->_hadUpdates = true;
        $this->_hadUpatesBeforeTransaction = true; // master becomes acrive even after ROLLBACK
        $this->_masterConnCache = $this->_getMasterConn(); // we'll never search for a master in the future
        $this->_slaveConnCache = null; // close the slave connection, and _hadUpdates==true, so never use slave
        return $this;
    }

    /**
     * Adds a namespace to the current SID.
     *
     * When null is pased, allows to execute updates on master without switching
     * a slave to the master after it. Also allows to execute non-update queries
     * on a slave independent to its state (force slave to be used).
     *
     * @param string $nsName
     * @return DB_Micro_IConnection
     */
    public function mSidNamespace($nsName)
    {
        $obj = clone $this;
        if ($nsName === null) {
            $obj->_sid = null;
            $obj->_hadUpdates = false;
            $obj->_hadUpatesBeforeTransaction = false;
        } else {
            $obj->_sid = $this->_sid . "." . $nsName;
        }
        return $obj;
    }

    public function query($sql, $args = array())
    {
        $conn = $this->_getPreferredSlaveConn();
        return $conn->query($sql, $args);
    }

    public function update($sql, $args = array())
    {
        $conn = $this->_getMasterConn();
        $result = $conn->update($sql, $args);
        $this->_hadUpdates = true; // we are here if no exception happened
        if (!$this->_inTransaction) {
            $this->_saveCurMasterPosIfHadUpdates();
        } else {
            // If we are inside a transaction, save the position
            // in commit handler only, not here.
        }
        return $result;
    }

    public function beginTransaction()
    {
        $conn = $this->_getMasterConn();
        $result = $conn->beginTransaction();
        $this->_hadUpatesBeforeTransaction = $this->_hadUpdates;
        $this->_hadUpdates = true; // transaction means we need an actual data! so use the master
        $this->_inTransaction = true;
        return $result;
    }

    public function commit()
    {
        $conn = $this->_getMasterConn();
        $result = $conn->commit();
        $this->_inTransaction = false;
        $this->_saveCurMasterPosIfHadUpdates();
        return $result;
    }

    public function rollBack()
    {
        $conn = $this->_getMasterConn();
        $result = $conn->rollBack();
        $this->_inTransaction = false;
        $this->_hadUpdates = $this->_hadUpatesBeforeTransaction; // go back to slave if we've been at the slave before BEGIN
        return $result;
    }

    public function getDsn()
    {
        return $this->_getPreferredSlaveConn()->getDsn();
    }

    /**
     * Returns current preferred slave (or master if we are switched to the master)
     * connection link. Used rarely to support native PHP functions which cannot
     * be called via DB_Micro_IConnection interface (e.g. pg_copy_from).
     *
     * If we need to receive a master connection exactly, use:
     * $conn->mSidNamespace(null)->switchToMaster()->getLink()
     *
     * @return resource
     */
    public function getLink()
    {
        return $this->_getPreferredSlaveConn()->getLink();
    }

    /**
     * @return DB_Micro_IConnection
     */
    public function _getMasterConn()
    {
        if (!$this->_masterConnCache) {
            $conn = $this->_getSlaveConn();
            $state = $this->_prepareReadOnlyConnection($conn);
            $this->_masterConnCache = $this->_createConnection($state->masterHost, true);
        }
        return $this->_masterConnCache;
    }


    /**
     * @return string
     */
    protected function _getClusterNameHash()
    {
        return md5($this->_dsn);
    }

    /**
     * @return void
     */
    private function _saveCurMasterPosIfHadUpdates()
    {
        if ($this->_sid == null) {
            return;
        }
        if ($this->_hadUpdates) {
            $pos = $this->_impl->getMasterPos($this->_getMasterConn());
            $this->_kvStorage->set($this->_sid, $pos);
        }
    }

    /**
     * @return DB_Micro_IConnection
     * @throws DB_Micro_Exception
     */
    private function _getPreferredSlaveConn()
    {
        if ($this->_hadUpdates) {
            return $this->_getMasterConn();
        }
        $conn = $this->_getSlaveConn();
        $state = $this->_prepareReadOnlyConnection($conn);
        if (!$state->isSlaveLaterThanMaster) {
            return $this->_getMasterConn();
        }
        return $conn;
    }

    /**
     * @return DB_Micro_IConnection
     * @throws DB_Micro_Exception
     */
    private function _getSlaveConn()
    {
        if ($this->_slaveConnCache) {
            return $this->_slaveConnCache;
        }
        // Receive a list of potentially alive replicas.
        // Note that the master connection is never marked as failed,
        // because its health is never checked - we use _getMasterConn()
        // to connect to the master by its hostname, there are no
        // health checks there. And it's good: the master should be
        // retried with no relations of its health status (because it
        // could revive in any second, no need to wait for minutes).
        $hosts = $this->_getClosestNonFailedHosts();
        $fallbackMasterConn = null;
        $failConnectExceptions = array();
        foreach ($hosts as $host) {
            try {
                $conn = $this->_createConnection($host);
            } catch (DB_Micro_Exception $e) {
                $failConnectExceptions[] = $e;
                $this->_markHostAsFailed($host, $e);
                continue;
            }
            $state = $this->_prepareReadOnlyConnection($conn); // switches to read-only
            if ($state->isSlave) {
                $this->_slaveConnCache = $conn;
                return $this->_slaveConnCache;
            } else {
                // We've connected to a master instead, continue searching for a
                // slave, but save the master's connection to fall back to it.
                if ($fallbackMasterConn) {
                    // Second master?! No more! We're in a test environment where
                    // everybody is a master, so just return the first one.
                    break;
                }
                $state->masterHost = $host; // save master hostname - it is null initially
                $state->isSlaveLaterThanMaster = true;
                $fallbackMasterConn = $conn;
                if ($this->_paramMasterAsReplica) {
                    // We've found a master and do not want to waste time searching
                    // for a slave (e.g. the master is in the same datacenter as we are),
                    // so use it.
                    break;
                } else {
                    continue;
                }
            }
        }
        // If all slaves are died, but we've found a master, use it.
        if ($fallbackMasterConn) {
            $this->_slaveConnCache = $fallbackMasterConn;
            return $this->_slaveConnCache;
        }
        // No servers available at all.
        $exceptionsText = array();
        if ($failConnectExceptions) {
            foreach ($failConnectExceptions as $e) {
                $exceptionsText[] = '- ' . ltrim($this->_shift($e->__toString()));
            }
        }
        throw new DB_Micro_Exception(
            "All databases seem to be inaccessible, cannot connect."
            . ($exceptionsText? "\nExceptions happened:\n" . join("\n", $exceptionsText) : ""),
            'connect'
        );
    }

    /**
     * @return array
     */
    private function _getClosestNonFailedHosts()
    {
        $hosts = $allHosts = $this->_impl->sortHostsByDistance($this->_dsnHosts);
        $failed = $this->_kvStorage->get($this->_getClusterNameHash());
        $notTryToConnectMessages = array();
        if (is_array($failed) && count($failed)) {
            foreach ($hosts as $i => $host) {
                if (isset($failed[$host]) && ($dt = $failed[$host] - time()) >= 0) {
                    $notTryToConnectMessages[] = self::LOG_PREFIX . "not trying to connect to \"$host\", because it is marked as failed for $dt second(s) more.";
                    unset($hosts[$i]);
                }
            }
        }
        if (!$hosts) {
            $this->_callLogger(self::LOG_PREFIX . "no alive hosts found at all, so we try to connect to all hosts.");
            $this->_markClusterAsAlive();
            $hosts = $allHosts;
        } else {
            foreach ($notTryToConnectMessages as $msg) {
                $this->_callLogger($msg);
            }
        }
        return array_values($hosts);
    }

    /**
     * @param string $host
     * @param Exception $e
     * @return void
     */
    private function _markHostAsFailed($host, Exception $e)
    {
        $k = $this->_getClusterNameHash();
        $failed = $this->_kvStorage->get($k);
        if (!$failed) {
            $failed = array();
        }
        $failed[$host] = time() + $this->_paramFailCheckInterval;
        $this->_kvStorage->set($k, $failed);
            $this->_callLogger(
                self::LOG_PREFIX . "marked host \"$host\" as failed for {$this->_paramFailCheckInterval} seconds.\n"
                . '- ' . ltrim($this->_shift(ucfirst($e->__toString())))
            );
    }

    /**
     * @return void
     */
    private function _markClusterAsAlive()
    {
        $this->_kvStorage->set($this->_getClusterNameHash(), null);
    }

    /**
     * @return mixed
     */
    private function _getLastCommitPos()
    {
        if ($this->_lastCommitPosCache === false) {
            $this->_lastCommitPosCache = $this->_sid !== null? $this->_kvStorage->get($this->_sid) : null;
            if (!$this->_lastCommitPosCache) {
                $this->_lastCommitPosCache = null;
            }
        }
        return $this->_lastCommitPosCache;
    }

    /**
     * @param DB_Micro_IConnection $conn
     * @return DB_Micro_Replication_SlaveState
     */
    private function _prepareReadOnlyConnection($conn)
    {
        $connId = $conn->__replicationConnId;
        if (!isset($this->_connStatesCache[$connId])) {
            $this->_connStatesCache[$connId] = $this->_impl->prepareReadOnlyConnection($conn, $this->_getLastCommitPos());
        }
        return $this->_connStatesCache[$connId];
    }

    /**
     * @param string $host
     * @param bool $isMaster
     * @return DB_Micro_IConnection
     */
    private function _createConnection($host, $isMaster = false)
    {
        static $counter = 1;
        $dsn = preg_replace('{\*}s', $host, $this->_dsnWithoutHost);
        if ($isMaster) {
            $dsn .= self::DSN_MASTER_SUFFIX;
        }
        $conn = $this->_impl->createConnection($dsn, $this->_logger);
        // We add a counter to DSN to uniquely identify connection objects
        // in $this->_connStatesCache cache. We can't identify objects by
        // DSNs, because many connections with same DSN, but with different
        // state may exist (e.g. on dev-server).
        $conn->__replicationConnId = $counter++;
        return $conn;
    }

    private function _callLogger($msg)
    {
        if ($this->_logger) {
            call_user_func($this->_logger, $msg);
        }
    }

    private function _shift($s)
    {
        return preg_replace('/^/m', '  ', $s);
    }
}
