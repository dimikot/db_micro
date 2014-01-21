<?php
class DB_Micro_Replication_StorageHealth_TmpFile extends DB_Micro_Replication_StorageHealth_Abstract
{
    private $_perms = 0666;

    public function setPerms($perms)
    {
        $this->_perms = $perms;
    }

    public function set($k, $v)
    {
        $fname = $this->_getDataFileName($k);
        $f = @fopen($fname, "a+");
        if (!$f) {
            $this->_throwError("Cannot open $fname for writing!");
        }
        flock($f, LOCK_EX);
        $old = umask(0);
        @chmod($fname, $this->_perms);
        umask($old);
        fseek($f, 0, SEEK_SET);
        ftruncate($f, 0);
        fwrite($f, $v);
        flock($f, LOCK_UN);
        fclose($f);
    }

    public function get($k)
    {
        $fname = $this->_getDataFileName($k);
        $f = @fopen($fname, "r");
        if (!$f) {
            return null;
        }
        flock($f, LOCK_SH);
        $r = stream_get_contents($f);
        flock($f, LOCK_UN);
        fclose($f);
        return $r;
    }

    private function _getDataFileName($k)
    {
        return sys_get_temp_dir() . "/" . __CLASS__ . "." . md5($k) . ".dat";
    }
}
