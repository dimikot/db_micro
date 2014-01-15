<?php
class DB_Micro_Replication_SlaveState
{
    /**
     * @var bool
     */
    public $isSlave;

    /**
     * @var string
     */
    public $masterHost;

    /**
     * @var bool
     */
    public $isSlaveLaterThanMaster;
}
