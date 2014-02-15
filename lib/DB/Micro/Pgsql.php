<?php
class DB_Micro_Pgsql extends DB_Micro_Abstract
{
    const CONN_RETRY_DELAY = 0.1;

    /**
     * Temporarily connect error buffer.
     *
     * @var string
     */
    private $_connectErrorMsg;

    /**
     * Last notice.
     *
     * @var string
     */
    private $_lastNotice = null;

    /**
     * OID of BYTEA type (cached).
     *
     * @var int
     */
    private $_byteaOid = null;

    /**
     * List of connection strings which was already used in the current script (in keys).
     */
    private static $_openedConnStrs = array();

    /**
     * Perform the connect operation given a parsed DSN.
     * If the connection fails, throws an exception.
     *
     * @param array $parsedDsn
     * @param int $numTries
     * @return resource
     * @throws DB_Micro_Exception
     */
    protected function _performConnect($parsedDsn, $numTries = 1)
    {
        if (!is_callable('pg_connect')) {
            throw new DB_Micro_ExceptionConnect("PostgreSQL extension is not loaded", "pg_connect");
        }
        $connStr = $this->_dsn2str($parsedDsn);
        set_error_handler(array($this, '_onConnectError'), E_WARNING);
        $link = null;
        for ($i = 0; !$link && $i < $numTries; $i++) {
            if ($i) {
                $dt = max(0, self::CONN_RETRY_DELAY - (microtime(true) - $prevConnectTime));
                if ($dt) usleep($dt * 1000000); // wait a bit before connection retry
            }
            $this->_connectErrorMsg = null;
            $prevConnectTime = microtime(true);
            if (!empty($parsedDsn['pconnect']) && empty(self::$_openedConnStrs[$connStr])) {
                // Pconnect mode is on AND we have NO other connection to the same DSN created recently.
                $link = @pg_pconnect($connStr);
                if ($link) {
                    if (in_array(@pg_transaction_status($link), array(PGSQL_TRANSACTION_INTRANS, PGSQL_TRANSACTION_INERROR))) {
                        // Typically we should never enter this "if" block, because transactions are
                        // rolled back by pgsql's module register_shutdown callback (see pgsql.c,
                        // function registered as PHP_RSHUTDOWN_FUNCTION). But if this shutdown
                        // function was not called for some reason, we do a manual rollback.
                        // ATTENTION! ROLLBACK goes before DISCARD, else DISCARD does not work!
                        @pg_query("ROLLBACK");
                    }
                    @pg_query("DISCARD ALL"); // "DISCARD ALL" cannot be glued with other queries!
                }
            } else {
                // Connection persistence is disabled. Or we already have a recently opened
                // connection, and we DO NOT WANT to reuse it at pg_* level, so we force creation
                // of a new connection.
                $link = @pg_connect($connStr, PGSQL_CONNECT_FORCE_NEW);
            }
        }
        restore_error_handler();
        if (!$link) {
            $msg = preg_replace('/\s*\[.*?\]/s', '', htmlspecialchars_decode($this->_connectErrorMsg))
                . " (tried $numTries times with " . self::CONN_RETRY_DELAY . "s delay)";
            throw new DB_Micro_ExceptionConnect($msg, "pg_connect");
        }
        self::$_openedConnStrs[$connStr] = true;
        return $link;
    }

    /**
     * Perform a plain query and return its result as array of rows.
     * If the query fails, throws an exception.
     *
     * @param resource $link
     * @param string $sql
     * @return array
     * @throws DB_Micro_Exception
     */
    protected function _performQuery($link, $sql)
    {
        if (!$this->_byteaOid) {
            // Get rid of pg_field_type() - use only pg_field_type_oid(),
            // so we need to fetch BYTEA OID once and cache it.
            $result = @pg_query($link, $tmpSql = "SELECT oid FROM pg_type WHERE typname='bytea'");
            if (!$result) {
                // DB_Micro_ExceptionConnect, because this query should never fail on a
                // healthy connection, ant it is a very first query.
                throw new DB_Micro_ExceptionConnect(pg_last_error($link), $tmpSql);
            }
            $this->_byteaOid = @pg_fetch_result($result, 0, 0);
            if (!$this->_byteaOid) {
                throw new DB_Micro_ExceptionConnect("Cannot fetch OID of BYTEA type - result is empty", $tmpSql);
            }
        }
        $result = @pg_query($link, $sql);
        if (!$result) {
            $error = pg_last_error($link);
            if (preg_match('/in a read-only transaction/s', $error)) {
                throw new DB_Micro_ExceptionReadonly($error, $sql);
            } else {
                throw new DB_Micro_Exception($error, $sql);
            }
        }
        $rows = array();
        while (($row = pg_fetch_assoc($result))) {
            $rows[] = $row;
        }
        // Convert BLOBs to DB_Micro_Blob("unescaped value").
        if ($rows) {
            $i = 0;
            foreach ($rows[0] as $k => $v) {
                // DO NOT use pg_field_type(), because it is damned slow!!!
                if (pg_field_type_oid($result, $i) == $this->_byteaOid) {
                    foreach ($rows as $n => $row) {
                        if ($row[$k] !== null) {
                            $rows[$n][$k] = new DB_Micro_Blob(pg_unescape_bytea($row[$k]));
                        }
                    }
                }
                $i++;
            }
        }
        return $rows;
    }

    /**
     * Perform literal quoting.
     *
     * @param resource $conn
     * @param string $value
     * @return string
     */
    protected function _performQuote($conn, $value)
    {
        if ($value === null) {
            $quoted = "NULL";
        } else if (is_object($value) && $value instanceof DB_Micro_Blob) {
            // Work-around to be compatible with 8.4 and 9.1+.
            $quoted = "decode('" . base64_encode($value->get()) . "', 'base64')";
        } else {
            // pg_escape_string does not support strings with \0 - it breaks
            // the string at that pos.
            $value = str_replace("\x00", " ", $value);
            $quoted = "'" . pg_escape_string($conn, $value) . "'";
            if (false !== strpos($quoted, '\\')) $quoted = 'E' . $quoted;
        }
        return $quoted;
    }

    /**
     * Convert parsed DSN to human-readable format for logging.
     * 
     * @param array $parsed
     * @return string
     * @throws DB_Micro_Exception
     */
    private function _dsn2str($parsed)
    {
        if (
            !isset($parsed['host'])
            || !isset($parsed['dbname']) || !isset($parsed['user'])
            || !isset($parsed['pass'])
        ) {
            throw new DB_Micro_ExceptionConnect("Invalid DSN format: {$parsed['dsn']}", '');
        }
        $str = '';
        $str .= "host='{$parsed['host']}'";
        if (!empty($parsed['port'])) {
            $str .= " port='{$parsed['port']}'";
        }
        if (isset($parsed['timeout'])) {
            $str .= " connect_timeout='{$parsed['timeout']}'"; // 'timeout' is deprecated
        }
        if (isset($parsed['connect_timeout'])) {
            $str .= " connect_timeout='{$parsed['connect_timeout']}'";
        }
        $str .= " dbname='{$parsed['dbname']}'";
        $str .= " user='{$parsed['user']}'";
        $str .= " password='{$parsed['pass']}'";
        return $str;
    }

    /**
     * This method is a error_handler hack for pg_connect(): we cannot
     * use any other method to fetch the error status of this function.
     *
     * Used internally only.
     *
     * @param int $num
     * @param string $msg
     * @return void
     */
    public function _onConnectError($num, $msg)
    {
        $num;
        $this->_connectErrorMsg = $msg;
    }

    /**
     * Fetch last notice generated by the database.
     *
     * @param resource $conn
     * @return string   Notice text or false if there is no notice generated.
     */
    protected function _performGetNotice($conn)
    {
        $return = pg_last_notice($conn);

        if ($return == $this->_lastNotice) {
            return false;
        }

        $this->_lastNotice = $return;

        return $return;
    }
}
