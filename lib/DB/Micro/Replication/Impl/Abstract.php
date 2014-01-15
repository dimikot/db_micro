<?php
abstract class DB_Micro_Replication_Impl_Abstract
{
    /**
     * @param string[] $hosts
     * @return string[]
     */
    abstract public function sortHostsByDistance(array $hosts);

    /**
     * @param string $host
     * @param callback $logger
     * @return DB_Micro_IConnection
     */
    abstract public function createConnection($host, $logger);

    /**
     * @param DB_Micro_IConnection $conn
     * @param mixed $masterPos
     * @return DB_Micro_Replication_SlaveState
     */
    abstract public function prepareReadOnlyConnection(DB_Micro_IConnection $conn, $masterPos);

    /**
     * @param DB_Micro_IConnection $conn
     * @return mixed
     */
    abstract public function getMasterPos(DB_Micro_IConnection $conn);
}
