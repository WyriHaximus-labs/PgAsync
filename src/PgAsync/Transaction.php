<?php

namespace PgAsync;

use Rx\Observable;

final class Transaction
{
    /** @var Connection */
    private $connection;

    /** @var callable */
    private $release;

    /** @var bool */
    private $finished = false;

    /**
     * @param callable $release
     */
    public function __construct(Connection $connection, callable $release)
    {
        $this->connection = $connection;
        $this->release    = $release;
    }

    public function query(string $queryString): Observable
    {
        if ($this->finished) {
            return $this->inactiveError();
        }

        return $this->connection->query($queryString);
    }

    public function executeStatement(string $queryString, array $parameters = []): Observable
    {
        if ($this->finished) {
            return $this->inactiveError();
        }

        return $this->connection->executeStatement($queryString, $parameters);
    }

    public function commit(): Observable
    {
        if ($this->finished) {
            return $this->inactiveError();
        }

        return $this->finishWithStatus('COMMIT', [$this, 'validateCommitStatus']);
    }

    public function rollback(): Observable
    {
        if ($this->finished) {
            return $this->inactiveError();
        }

        return $this->finishWithStatus('ROLLBACK', [$this, 'validateRollbackStatus']);
    }

    private function finishWithStatus(string $sql, callable $validate): Observable
    {
        return $this->connection->queryUntilReady($sql)->flatMap(function ($status) use ($validate) {
            $error = $validate($status);
            if ($error !== null) {
                return Observable::error($error);
            }

            $this->markFinished();

            return Observable::of($status);
        });
    }

    /**
     * @return \Throwable|null
     */
    private function validateCommitStatus(string $status)
    {
        if ($status === 'E') {
            return new \RuntimeException('Cannot commit: transaction is in failed state');
        }

        if ($status !== 'I') {
            return new \RuntimeException('Expected idle status after COMMIT');
        }

        return null;
    }

    /**
     * @return \Throwable|null
     */
    private function validateRollbackStatus(string $status)
    {
        if ($status !== 'I') {
            return new \RuntimeException('Expected idle status after ROLLBACK');
        }

        return null;
    }

    private function inactiveError(): Observable
    {
        return Observable::error(new \RuntimeException('Transaction has already been committed or rolled back'));
    }

    private function markFinished()
    {
        $this->finished = true;
        ($this->release)();
    }
}
