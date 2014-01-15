<?php
class DB_Micro_Replication_Impl_Pgsql extends DB_Micro_Replication_Impl_Abstract
{
    /**
     * Define this procedure with "SECURITY DEFINER" (!) privileges as:
     *
       CREATE OR REPLACE FUNCTION replication.replication_get_master_host() RETURNS text AS $body$
       DECLARE
           rc TEXT;
           h TEXT;
       BEGIN
           IF EXISTS(SELECT 1 FROM pg_stat_replication) THEN
               RETURN NULL;
           END IF;
           BEGIN
               rc := pg_read_file('recovery.conf');
           EXCEPTION
               WHEN others THEN RETURN NULL;
           END;
           h := substring(rc FROM E'(?n)^\\s*primary_conninfo\\s*=\\s*.*?host=([^\\s\'"]+)');
           IF h IS NULL THEN
               RETURN NULL;
           END IF;
           RETURN h;
       END;
       $body$ LANGUAGE 'plpgsql' VOLATILE CALLED ON NULL INPUT SECURITY DEFINER;
     *
     */
    const GET_MASTER_HOST_PROC_NAME = 'replication_get_master_host';

    /**
     * @param string[] $hosts
     * @return string[]
     */
    public function sortHostsByDistance(array $hosts)
    {
        return $hosts;
    }

    /**
     * @param string $dsn
     * @param callback $logger
     * @return DB_Micro_IConnection
     */
    public function createConnection($dsn, $logger)
    {
        $conn = new DB_Micro_Pgsql($dsn, $logger);
        return $conn;
    }

    /**
     * @param DB_Micro_IConnection $conn
     * @param mixed $masterPos
     * @return DB_Micro_Replication_SlaveState
     */
    public function prepareReadOnlyConnection(DB_Micro_IConnection $conn, $masterPos)
    {
        $result = $conn->query(""
            . "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY; "
            . "SELECT "
            . self::GET_MASTER_HOST_PROC_NAME . "() AS master_host, "
            . "(txid_snapshot_xmax(txid_current_snapshot()) >= ?::BIGINT)::INTEGER AS is_later",
            array($masterPos)
        );
        $result = current($result);
        $state = new DB_Micro_Replication_SlaveState();
        $state->masterHost = $result['master_host'];
        $state->isSlaveLaterThanMaster = $masterPos? !!$result['is_later'] : true;
        $state->isSlave = $state->masterHost !== null;
        return $state;
    }

    /**
     * @param DB_Micro_IConnection $conn
     * @return mixed
     */
    public function getMasterPos(DB_Micro_IConnection $conn)
    {
        $result = $conn->query('SELECT txid_current()');
        return current(current($result));
    }
}
