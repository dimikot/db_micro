<?php
class DB_Micro_Replication_KeyValueStorage_ZendCache extends DB_Micro_Replication_KeyValueStorage_Abstract
{
    private $_backend;

    public function __construct(Zend_Cache_Backend_Interface $backend)
    {
        $this->_backend = $backend;
    }

    public function get($k)
    {
        return $this->_backend->load($this->_getId($k));
    }

    public function set($k, $v)
    {
        $this->_backend->save($v, $this->_getId($k));
    }

    private function _getId($k)
    {
        return __CLASS__ . ':' . $k;
    }
}
