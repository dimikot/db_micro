<?php
class DB_Micro_Replication_StoragePos_Session extends DB_Micro_Replication_StoragePos_Abstract
{
    public function __construct()
    {
        if (!@session_id()) {
            $this->_throwError("An active session is required to save the current master's replication position.");
        }
    }

    public function set($k, $v)
    {
        $mangled = $this->_mangleKey($k);
        $_SESSION[$mangled] = $v;
    }

    public function get($k)
    {
        $mangled = $this->_mangleKey($k);
        return isset($_SESSION[$mangled])? $_SESSION[$mangled] : null;
    }

    private function _mangleKey($k)
    {
        return __CLASS__ . ':' . $k;
    }
}
