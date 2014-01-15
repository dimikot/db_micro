<?php
abstract class DB_Micro_Replication_KeyValueStorage_Abstract
{
    abstract public function get($k);
    abstract public function set($k, $v);
}
