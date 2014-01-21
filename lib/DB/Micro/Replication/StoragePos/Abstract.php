<?php
abstract class DB_Micro_Replication_StoragePos_Abstract
{
    /**
     * @param string $k
     * @param string $v
     * @return void
     */
    abstract public function set($k, $v);

    /**
     * @param string $k
     * @return string
     */
    abstract public function get($k);

    /**
     * @param string $msg
     * @throws Exception
     */
    protected function _throwError($msg)
    {
        throw new Exception($msg);
    }
}
