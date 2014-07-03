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
        $link = $prevConnectTime = null;
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
        if (pg_connection_status($link) !== PGSQL_CONNECTION_OK) {
            throw new DB_Micro_Exception("DB_Micro: remote side had already closed the connection before the query arrived", $sql);
        }
        $result = @pg_query($link, $sql);
        if (!$result) {
            $error = pg_last_error($link);
            if (!trim($error) && pg_connection_status($link) !== PGSQL_CONNECTION_OK) {
                // This happens when we are a waiting parent process AND the conection
                // has been closed inside a child process.
                throw new DB_Micro_Exception("DB_Micro: remote side has closed the connection before or during the query execution", $sql);
            }
            if (preg_match('/in a read-only transaction/s', $error)) {
                throw new DB_Micro_ExceptionReadonly($error, $sql);
            }
            throw new DB_Micro_Exception($error, $sql);
        }
        $rows = array();
        while (($row = pg_fetch_assoc($result))) {
            $rows[] = $row;
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
        } else {
            // pg_escape_string does not support strings with \0 - it breaks
            // the string at that pos.
            $value = str_replace(chr(0), " ", $value);
            $quoted = "'" . pg_escape_string($conn, $value) . "'";
            if (false !== strpos($quoted, '\\') && pg_escape_string($conn, "\\") !== "\\") {
                // If standard_conforming_strings=false and we have a slash within the
                // string, we must prepend "E", else we receive a pg_last_notice():
                // "WARNING: nonstandard use of \ in a string literal"
                $quoted = 'E' . $quoted;
            }
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
