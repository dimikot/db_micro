<?php
class DB_Micro_Test_ReplicationTest extends PHPUnit_Framework_TestCase
{
    /**
     * @var DB_Micro_Test_ReplicationTest_KeyValueStorage
     */
    private $_kvStorage;

    /**
     * @var array
     */
    private $_log = array();

    /**
     * @var callback
     */
    private $_logger;

    public function setUp()
    {
        $this->_kvStorage = new DB_Micro_Test_ReplicationTest_KeyValueStorage();
        $log =& $this->_log;
        $this->_logger = function($sql, $time=null) use (&$log) {
            // Remain only first line of error messages, because next lines typically
            // contain stacktraces, nested exceptions etc.
            $sql = preg_replace("/\r?\n.*$/s", '', $sql);
            $log[] = $sql;
        };
    }

    public function testDsnAndLinkDependsOnCurrentPreferredAndActiveSlaveOrMaster()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave1,slave2/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $this->assertRegExp('{/slave1/}', $replication->getDsn());
        $this->assertRegExp('{^link\(.*/slave1/.*\)$}', $replication->getLink());
        $replication->switchToMaster();
        $this->assertRegExp('{/master/}', $replication->getDsn());
        $this->assertRegExp('{^link\(.*/master/.*\)$}', $replication->getLink());
    }

    public function testConnectSlaveWhenItIsFirstAndAlive()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,b/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1'
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    /**
     * @group slow
     */
    public function testConnectSlaveWhenFirstSlaveIsDeadAndSecondIsAliveSoFirstIsMarkedAsFailedForOneSecond()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        // First connection - tries to connect sequentially.
        $replication = new DB_Micro_Test_ReplicationTest_Replication($dsn = 'p://dead,slave,b/test?fail_check_interval=1', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'dead: connect',
                'Replication: marked host "dead" as failed for 1 seconds.',
                'slave: connect',
                'slave(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get db', 'set db', 'get abcd'), $this->_kvStorage->ops);
        // Second connection - it knows that "dead" host is dead already, so doesn't try it.
        $this->_log = array();
        $this->_kvStorage->ops = array();
        $replication = new DB_Micro_Test_ReplicationTest_Replication($dsn, $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'Replication: not trying to connect to "dead", because it is marked as failed for 1 second(s) more.',
                'slave: connect',
                'slave(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
        // Third connection after a delay - dead slave is checked again.
        sleep(2);
        $this->_log = array();
        $this->_kvStorage->ops = array();
        $replication = new DB_Micro_Test_ReplicationTest_Replication($dsn, $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'dead: connect', // again
                'Replication: marked host "dead" as failed for 1 seconds.',
                'slave: connect',
                'slave(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get db', 'set db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testConnectSlaveWhenFirstIsMasterAndSecondIsSlave()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://master,slave/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'master: connect',
                'slave: connect',
                'slave(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testConnectWhenThreeMastersInDsnStopTryingOnSecond()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://master1,master2,master3/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'master1: connect',
                'master2: connect',
                // no master3!
                'master1(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testConnectWhenFirstIsMasterAndMasterAsReplicaParamIsSetLetMasterBeUsedAsReplica()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://master,slave/test?master_as_replica=1', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'master: connect',
                // no slave!
                'master(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    /**
     * @group testConnectMasterAsReadOnlyWhenAllReplicasAreDeadAndOnlyMasterLives
     */
    public function testConnectMasterAsReadOnlyWhenAllReplicasAreDeadAndOnlyMasterLives()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://dead1,dead2,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'dead1: connect',
                'Replication: marked host "dead1" as failed for 600 seconds.',
                'dead2: connect',
                'Replication: marked host "dead2" as failed for 600 seconds.',
                'master: connect',
                'master(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get db', 'set db', 'get db', 'set db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testTwoQueriesToSlave()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1',
                'slave(ro): select 2',
            ),
            $this->_log
        );
        $this->assertEquals(array(), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testUpdateThenQueryToMaster()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $replication->update('update t1');
        $replication->update('update t2');
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1',
                'master: connect',
                'master: update t1',
                'master: select master pos',
                'master: update t2',
                'master: select master pos',
                'master: select 2',
            ),
            $this->_log
        );
        $this->assertEquals(array('abcd' => 2), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd', 'set abcd', 'set abcd'), $this->_kvStorage->ops);
    }

    public function testUpdateSavePosOnCommitThenQueryToMaster()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 0');
        $replication->beginTransaction();
        $replication->query('select 1');
        $replication->update('update t1');
        $replication->update('update t2');
        $replication->query('select 2');
        $replication->commit();
        $replication->query('select 3');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 0',
                'master: connect',
                'master: BEGIN',
                'master: select 1',
                'master: update t1',
                'master: update t2',
                'master: select 2',
                'master: COMMIT',
                'master: select master pos',
                'master: select 3',
            ),
            $this->_log
        );
        $this->assertEquals(array('abcd' => 2), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd', 'set abcd'), $this->_kvStorage->ops);
    }

    public function testUpdateSavePosNextConnectIsWithLagSoUseMaster()
    {
        // First connect - save the data and non-zero master pos.
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->update('update t1');
        $this->assertEquals(
            array(
                'slave: connect',
                'master: connect',
                'master: update t1',
                'master: select master pos',
            ),
            $this->_log
        );
        $this->assertEquals(array('abcd' => 1), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd', 'set abcd'), $this->_kvStorage->ops);
        // Second connect - slave is in lag for 'abcd' sid, so use master.
        $this->_log = array();
        $this->_kvStorage->ops = array();
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave_with_pos_0,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'slave_with_pos_0: connect',
                'master: connect',
                'master: select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testWhenReplicaActiveAfterBeginGoToMasterAfterRollbackReturnToReplica()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->beginTransaction();
        $replication->update('update t1');
        $replication->rollBack();
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'slave: connect',
                'master: connect',
                'master: BEGIN',
                'master: update t1',
                'master: ROLLBACK',
                'slave(ro): select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array(), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd'), $this->_kvStorage->ops);
    }

    public function testWhenMasterActiveAfterBeginGoToMasterAfterRollbackStaysOnMaster()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->update('update t1');
        $replication->beginTransaction();
        $replication->update('update t2');
        $replication->rollBack();
        $replication->query('select 1');
        $this->assertEquals(
            array(
                'slave: connect',
                'master: connect',
                'master: update t1',
                'master: select master pos',
                'master: BEGIN',
                'master: update t2',
                'master: ROLLBACK',
                'master: select 1',
            ),
            $this->_log
        );
        $this->assertEquals(array('abcd' => 1), $this->_kvStorage->storage);
        $this->assertEquals(array('get db', 'get abcd', 'set abcd'), $this->_kvStorage->ops);
    }

    public function testSwitchToMaster()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $replication->switchToMaster();
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1',
                'master: connect',
                'master: select 2',
            ),
            $this->_log
        );
    }

    /**
     * @group testSwitchToMasterAfterFirstQueryClosesSlaveConnAndRemainsOnlyMasterConnToEconomizeConnections
     */
    public function testSwitchToMasterAfterFirstQueryClosesSlaveConnAndRemainsOnlyMasterConnToEconomizeConnections()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 0');
        $replication->switchToMaster();
        $replication->query('select 1');
        $replication->mSidNamespace(null)->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 0',
                'master: connect',
                'master: select 1',
                'slave: connect',
                'slave(ro): select 2',
            ),
            $this->_log
        );
    }

    /**
     * @group testSwitchToMasterWithNoQueriesBeforeClosesSlaveConnAndRemainsOnlyMasterConnToEconomizeConnections
     */
    public function testSwitchToMasterWithNoQueriesBeforeClosesSlaveConnAndRemainsOnlyMasterConnToEconomizeConnections()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->switchToMaster();
        $replication->query('select 1');
        $replication->mSidNamespace(null)->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'master: connect',
                'master: select 1',
                'slave: connect',
                'slave(ro): select 2',
            ),
            $this->_log
        );
    }

    public function testSwitchToMasterInsideRolledBackTransaction()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $replication->beginTransaction();
        $replication->update('update t1');
        $replication->switchToMaster();
        $replication->rollBack();
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1',
                'master: connect',
                'master: BEGIN',
                'master: update t1',
                'master: ROLLBACK',
                'master: select 2',
            ),
            $this->_log
        );
    }

    public function testMSidNamespaceUpdate()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->query('select 1');
        $replication->mSidNamespace(null)->update('update t1');
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'slave(ro): select 1',
                'master: connect',
                'master: update t1',
                'slave(ro): select 2',
            ),
            $this->_log
        );
    }

    public function testMSidNamespaceSelect()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        $replication = new DB_Micro_Test_ReplicationTest_Replication('p://slave,master/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
        $replication->update('update t1');
        $replication->mSidNamespace(null)->query('select 1');
        $replication->query('select 2');
        $this->assertEquals(
            array(
                'slave: connect',
                'master: connect',
                'master: update t1',
                'master: select master pos',
                'slave(ro): select 1',
                'master: select 2',
            ),
            $this->_log
        );
    }

    /**
     * @group testConnectEveryoneWhenAllHostsAreDead
     */
    public function testConnectEveryoneWhenAllHostsAreDead()
    {
        $impl = new DB_Micro_Test_ReplicationTest_Impl();
        try {
            $replication = new DB_Micro_Test_ReplicationTest_Replication('p://dead1,dead2/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
            $replication->query('select 1');
            $this->fail("Should be a DB_Micro_Exception.");
        } catch (DB_Micro_Exception $e) {
            $this->assertEquals(
                array(
                    'dead1: connect',
                    'Replication: marked host "dead1" as failed for 600 seconds.',
                    'dead2: connect',
                    'Replication: marked host "dead2" as failed for 600 seconds.',
                ),
                $this->_log
            );
            $this->assertEquals(array('get db', 'get db', 'set db', 'get db', 'set db'), $this->_kvStorage->ops);
        }
        $this->_log = array();
        $this->_kvStorage->ops = array();
        try {
            $replication = new DB_Micro_Test_ReplicationTest_Replication('p://dead1,dead2/test', $this->_logger, 'abcd', $impl, $this->_kvStorage);
            $replication->query('select 1');
            $this->fail("Should be a DB_Micro_Exception.");
        } catch (DB_Micro_Exception $e) {
            $this->assertEquals(
                array(
                    'Replication: no alive hosts found at all, so we try to connect to all hosts.',
                    'dead1: connect',
                    'Replication: marked host "dead1" as failed for 600 seconds.',
                    'dead2: connect',
                    'Replication: marked host "dead2" as failed for 600 seconds.',
                ),
                $this->_log
            );
            $this->assertEquals(array('get db', 'set db', 'get db', 'set db', 'get db', 'set db'), $this->_kvStorage->ops);
        }
    }
}


class DB_Micro_Test_ReplicationTest_Replication extends DB_Micro_Replication
{
    protected function _getClusterNameHash()
    {
        return 'db';
    }
}


class DB_Micro_Test_ReplicationTest_KeyValueStorage extends DB_Micro_Replication_KeyValueStorage_Abstract
{
    public $storage = array();
    public $ops = array();

    public function get($k)
    {
        $this->ops[] = "get $k";
        return @$this->storage[$k];
    }

    public function set($k, $v)
    {
        $this->ops[] = "set $k";
        $this->storage[$k] = $v;
    }
}


class DB_Micro_Test_ReplicationTest_Impl extends DB_Micro_Replication_Impl_Abstract
{
    public function sortHostsByDistance(array $hosts)
    {
        return $hosts;
    }

    public function createConnection($dsn, $logger)
    {
        return new DB_Micro_Test_ReplicationTest_Connection($dsn, $logger);
    }

    public function prepareReadOnlyConnection(DB_Micro_IConnection $conn, $masterPos)
    {
        $conn->isReadOnly = true;
        $state = new DB_Micro_Replication_SlaveState();
        if (preg_match('/master/', $conn->getDsn())) {
            $state->isSlave = false;
            return $state;
        }
        $state->isSlave = true;
        $state->masterHost = "master";
        if (preg_match('/slave_with_pos_(\d+)/s', $conn->getDsn(), $m)) {
            $state->isSlaveLaterThanMaster = $m[1] >= $masterPos;
        } else {
            $state->isSlaveLaterThanMaster = true;
        }
        return $state;
    }

    public function getMasterPos(DB_Micro_IConnection $conn)
    {
        $conn->query('select master pos');
        return $conn->numUpdates;
    }
}


class DB_Micro_Test_ReplicationTest_Connection implements DB_Micro_IConnection
{
    public $dsn;
    public $logger;
    public $numUpdates = 0;
    public $isReadOnly = false;

    public function __construct($dsn, $logger)
    {
        $this->dsn = $dsn;
        $isReadOnly =& $this->isReadOnly; // changed from outside
        $this->logger = function ($sql) use($dsn, $logger, &$isReadOnly) {
            $host = parse_url($dsn, PHP_URL_HOST);
            call_user_func($logger, $host . ($isReadOnly? '(ro)' : '') . ": " . $sql);
        };
        call_user_func($this->logger, 'connect');
        if (preg_match('/dead/s', $dsn)) {
            throw new DB_Micro_Exception("DSN is dead", "connect");
        }
    }

    public function query($sql, $args = array())
    {
        call_user_func($this->logger, $sql);
    }

    public function update($sql, $args = array())
    {
        $this->numUpdates++;
        call_user_func($this->logger, $sql);
    }

    public function beginTransaction()
    {
        call_user_func($this->logger, 'BEGIN');
    }

    public function commit()
    {
        call_user_func($this->logger, 'COMMIT');
    }

    public function rollBack()
    {
        call_user_func($this->logger, 'ROLLBACK');
    }

    public function getDsn()
    {
        return $this->dsn;
    }

    public function getLink()
    {
        return 'link(' . $this->dsn . ')';
    }
}