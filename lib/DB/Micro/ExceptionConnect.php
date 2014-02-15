<?php
class DB_Micro_ExceptionConnect extends DB_Micro_Exception
{
    private $_isMaster = false;

    public function setIsMaster($flag)
    {
        $this->_isMaster = $flag;
    }

    public function isMaster()
    {
        return $this->_isMaster;
    }
}
