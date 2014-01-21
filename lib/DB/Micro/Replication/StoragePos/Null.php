<?php
class DB_Micro_Replication_StoragePos_Null extends DB_Micro_Replication_StoragePos_Abstract
{
    public function set($k, $v)
    {
    }

    public function get($k)
    {
        return null;
    }
}
