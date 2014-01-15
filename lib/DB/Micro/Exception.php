<?php
class DB_Micro_Exception extends Exception
{
    private $_query;

    public function __construct($message, $query)
    {
        $this->_query = $query;
        parent::__construct($message . "\nSQL: " . $this->_query);
    }
    
    public function setMessage($message)
    {
        $this->message = $message;
    }
}