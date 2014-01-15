<?php
interface DB_Micro_IConnection
{
    /**
     * Performs an SQL query with arguments and returns array of objects
     * of resulting rows.
     *
     * @param string $sql
     * @param array $args
     * @return array
     */
    public function query($sql, $args = array());

    /**
     * Performs an SQL query which modifies data.
     *
     * @param string $sql
     * @param array $args
     * @return array
     */
    public function update($sql, $args = array());

    public function beginTransaction();

    public function commit();

    public function rollBack();

    public function getDsn();

    public function getLink();
}
