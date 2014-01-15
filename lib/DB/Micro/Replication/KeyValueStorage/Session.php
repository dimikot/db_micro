<?php
class DB_Micro_Replication_KeyValueStorage_Session extends DB_Micro_Replication_KeyValueStorage_Abstract
{
    /**
     * @var Zend_Session_Namespace
     */
    private $_ns;

    public function __construct(Zend_Session_Namespace $ns)
    {
        $this->_ns = $ns;
    }

    public function get($k)
    {
        return $this->_ns->$k;
    }

    public function set($k, $v)
    {
        $this->_ns->$k = $v;
    }
}
