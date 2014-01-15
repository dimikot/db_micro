<?php
class DB_Micro_Blob
{
    private $_value;
    
    public function __construct($value)
    {
        $this->_value = $value;
    }
    
    public function get()
    {
        return $this->_value;
    }
}