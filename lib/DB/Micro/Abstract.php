<?php
abstract class DB_Micro_Abstract implements DB_Micro_IConnection
{
    private static $_numConn = 0;

    private $_tmpQuery;
    private $_tmpArgs;
    private $_tmpArgsPos;

    private $_logger;
    private $_conn;
    private $_connName;
    private $_dsn;
    private $_connectPerformed;

    /**
     * Perform the connect operation given a parsed DSN.
     * If the connection fails, throws an exception.
     *
     * @param array $parsedDsn
     * @param int $numTries
     * @return resource
     */
    abstract protected function _performConnect($parsedDsn, $numTries = 1);

    /**
     * Perform a plain query and return its result as array of rows.
     * If the query fails, throws an exception.
     *
     * @param resource $conn
     * @param string $sql
     * @return array
     */
    abstract protected function _performQuery($conn, $sql);

    /**
     * Perform a data modifying query.
     *
     * @param resource $conn
     * @param string $sql
     * @return array
     */
    protected function _performUpdate($conn, $sql)
    {
        // By default - redirect to the plain query.
        return $this->_performQuery($conn, $sql);
    }

    /**
     * Perform get notice.
     * Non Abstract because not all DB support raising notice...
     *
     * @param resource $conn
     * @return array
     */
    protected function _performGetNotice($conn)
    {
    }

    /**
     * Perform literal quoting.
     *
     * @param resource $conn
     * @param string $value
     * @return string
     */
    abstract protected function _performQuote($conn, $value);

    /**
     * Create a new connection.
     *
     * @param mixed $dsn         DSN to connect to OR native PHP link resource.
     * @param callback $logger   Callback for logging: function($query, $tookTime)
     * @throws DB_Micro_Exception
     */
    public function __construct($dsn, $logger = null)
    {
        $this->_dsn = $dsn;
        $this->_logger = $logger;
        $t0 = microtime(true);
        try {
            if (!is_resource($dsn)) {
                $parsed = self::parseDSN($dsn);
                $numTries = @$parsed['num_conn_tries'];
                $this->_connName = $parsed['connName'] . "#" . (++self::$_numConn);
                $this->_conn = $this->_performConnect($parsed, $numTries? $numTries : 1);
                $this->_connectPerformed = true;
            } else {
                $this->_connName = strval($dsn) . "#" . (++self::$_numConn);
                $this->_conn = $dsn;
            }
        } catch (DB_Micro_Exception $e) {
            // Exception during the query: log it.
            $dt = microtime(true) - $t0;
            $this->_log("CONNECT '" . addslashes($this->_connName) . "'", $dt, null, $e->getMessage());
            throw $e;
        }
        $dt = microtime(true) - $t0;
        $this->_log("CONNECT '" . addslashes($this->_connName) . "'", $dt, array(array(1)));
    }

    /**
     * Just logs that the connection has been closed.
     */
    public function __destruct()
    {
        if ($this->_connectPerformed) {
            $this->_log("DISCONNECT '" . addslashes($this->_connName) . "'", 0, array(array(1)));
        }
    }

    /**
     * Performs an SQL query with arguments and returns array of objects
     * of resulting rows.
     *
     * @param string $sql
     * @param array $args
     * @return array
     */
    public function query($sql, $args = array())
    {
        return $this->_query($sql, $args, false);
    }

    /**
     * Performs an SQL query which modifies data.
     *
     * @param string $sql
     * @param array $args
     * @return array
     */
    public function update($sql, $args = array())
    {
        return $this->_query($sql, $args, true);
    }

    public function beginTransaction()
    {
        return $this->query('BEGIN');
    }

    public function commit()
    {
        return $this->query('COMMIT');
    }

    public function rollBack()
    {
        return $this->query('ROLLBACK');
    }

    public function getDsn()
    {
        return $this->_dsn;
    }

    public function getLink()
    {
        return $this->_conn;
    }

    /**
     * Parse a data source name. See parse_url() for details.
     * This method adds the following keys:
     * - 'dbname' (and removes 'path')
     * - 'dsn'
     * - 'connName'
     * - removes 'query', but adds its parsed parameters
     *
     * @param string $dsn
     * @return array
     */
    public static function parseDSN($dsn)
    {
        $parsed = @parse_url($dsn);
        if (!$parsed) return null;
        $parsed['dbname'] = ltrim($parsed['path'], '/');
        if (!empty($parsed['query'])) {
            $params = null;
            parse_str($parsed['query'], $params);
            $parsed += $params;
        }
        $parsed['dsn'] = $dsn;
        if (isset($parsed['dbproxy'])) {
            // The "dbproxy" parameter causes prepending the database name by the
            // host name and switching host name to the proxy's host. E.g. the DSN
            //   pgsql://host/db?dbproxy=127.0.0.1
            // is converted to
            //   pgsql://127.0.0.1/host-db
            $parsed['dbname'] = $parsed['host'] . (@$parsed['port']? '-' . $parsed['port'] : '') . '-' . ltrim($parsed['dbname']);
            $parsed['host'] = $parsed['dbproxy'];
            if (preg_match('/^(.*):(\d+)$/s', $parsed['host'], $m)) {
                $parsed['host'] = $m[1];
                $parsed['port'] = $m[2];
            }
            unset($params['dbproxy']);
            $parsed['query'] = http_build_query($params);
        }
        $parsed['connName'] = ""
            . "{$parsed['scheme']}://"
            . "{$parsed['user']}@{$parsed['host']}"
            . (@$parsed['port']? ":{$parsed['port']}" : "")
            . "/{$parsed['dbname']}"
            . (@$parsed['query']? "?{$parsed['query']}" : "")
            . (@$parsed['fragment']? "#{$parsed['fragment']}" : "");
        unset($parsed['query']);
        unset($parsed['path']);
        return $parsed;
    }

    /**
     * Wrapper for choosing _performQuery/_performUpdate.
     *
     * @param string $sql
     * @param array $args
     * @param bool $isUpdate
     * @return array
     * @throws DB_Micro_Exception
     */
    private function _query($sql, $args = array(), $isUpdate)
    {
        // Build the complete query.
        $this->_tmpQuery = $sql;
        $this->_tmpArgs = $args;
        $this->_tmpArgsPos = 0;
        $query = preg_replace_callback('/\?/s', array($this, '_queryReplacer'), $sql);

        // Perform the query.
        $t0 = microtime(true);
        try {
            $rows = $isUpdate ? $this->_performUpdate($this->_conn, $query) : $this->_performQuery($this->_conn, $query);
        } catch (DB_Micro_Exception $e) {
            // Exception during the query: log it.
            $dt = microtime(true) - $t0;
            $this->_log($query, $dt, null, $e->getMessage(), $this->_performGetNotice($this->_conn));
            throw $e;
        }

        // Log the query and its result.
        $dt = microtime(true) - $t0;
        $this->_log($query, $dt, $rows, null, $this->_performGetNotice($this->_conn));

        // Return the resulting rows.
        return $rows;
    }

    /**
     * Internal method to replace placeholders.
     *
     * @return string
     * @throws DB_Micro_Exception
     */
    public function _queryReplacer()
    {
        if ($this->_tmpArgsPos >= count($this->_tmpArgs)) {
            throw new DB_Micro_Exception("Parameter #{$this->_tmpArgsPos} is not passed", $this->_tmpQuery);
        }
        $value = $this->_tmpArgs[$this->_tmpArgsPos++];
        return $this->_performQuote($this->_conn, $value);
    }

    /**
     * Log a query result.
     *
     * @param string $sql
     * @param float $time
     * @param array $result
     * @param string $error
     * @param string $notice
     */
    private function _log($sql, $time, $result, $error = null, $notice = null)
    {
        if (!$this->_logger) {
            return;
        }
        $this->_callLogger(rtrim($sql) . ";", $time);
        $this->_callLogger(sprintf(
            '-- %d ms; %s; %s',
            $time * 1000,
            $error ? "ERROR" : count($result) . " row(s)",
            $this->_connName
        ));

        if ($notice) {
            $this->_callLogger("-- " . trim($notice));
        }

        if ($error) {
            foreach (explode("\n", $error) as $line) {
                $this->_callLogger("-- " . trim($line));
            }
        }

        if ($notice || $error) {
            $backtrace = debug_backtrace();
            $text = "-- Backtrace:\n";
            $items = array();
            foreach (array_reverse($backtrace) as $step) {
                if (@$step['file'] == __FILE__) break;
                $items[] = $step;
            }
            foreach (array_reverse($items) as $i => $step) {
                $text .= sprintf(
                    "-- %02d: %s %s\n",
                    $i,
                    empty($step['file']) ? '... ' : $step['file'] . ':' . $step['line'],
                    !empty($step['class']) ? @$step['class'] . '::' . @$step['function'] : @$step['function']
                );
            }
            $this->_callLogger(rtrim($text));
        }
    }

    private function _callLogger($msg, $time=null)
    {
        call_user_func($this->_logger, $msg, $time, $this->_connName);
    }
}
