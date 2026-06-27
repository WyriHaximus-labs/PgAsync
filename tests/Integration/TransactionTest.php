<?php

namespace PgAsync\Tests\Integration;

use PgAsync\Client;
use PgAsync\Transaction;
use Rx\Observable;
use Rx\Observer\CallbackObserver;

class TransactionTest extends TestCase
{
    private function client(array $parameters = []): Client
    {
        return self::clientFromEnv(array_merge([
            'user'     => $this->getDbUser(),
            'password' => $this::getDbUser(),
            'database' => $this::getDbName(),
        ], $parameters), $this->getLoop());
    }

    private function countThings(Client $client, string $thingType): Observable
    {
        return $client->executeStatement(
            'SELECT COUNT(*) AS c FROM thing WHERE thing_type = $1',
            [$thingType]
        );
    }

    public function testCommitPersistsChanges()
    {
        $client = $this->client();
        $thingType = 'txn_commit_' . uniqid();
        $count = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use ($thingType) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_cost) VALUES ($1, $2)',
                [$thingType, 1.00]
            )->concat($tx->commit());
        })->flatMap(function () use ($client, $thingType) {
            return $this->countThings($client, $thingType);
        })->subscribe(new CallbackObserver(
            function ($row) use (&$count) {
                $count = (int) $row['c'];
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Count query failed: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame(1, $count);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testRollbackDiscardsChanges()
    {
        $client = $this->client();
        $thingType = 'txn_rollback_' . uniqid();
        $count = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use ($thingType) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_cost) VALUES ($1, $2)',
                [$thingType, 1.00]
            )->concat($tx->rollback());
        })->flatMap(function () use ($client, $thingType) {
            return $this->countThings($client, $thingType);
        })->subscribe(new CallbackObserver(
            function ($row) use (&$count) {
                $count = (int) $row['c'];
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Count query failed: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame(0, $count);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testExecuteStatementInsideTransaction()
    {
        $client = $this->client();
        $thingType = 'txn_stmt_' . uniqid();
        $count = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use ($thingType) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_description, thing_cost) VALUES ($1, $2, $3)',
                [$thingType, 'inside transaction', 9.99]
            )->concat($tx->commit());
        })->flatMap(function () use ($client, $thingType) {
            return $this->countThings($client, $thingType);
        })->subscribe(new CallbackObserver(
            function ($row) use (&$count) {
                $count = (int) $row['c'];
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Count query failed: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame(1, $count);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testConcurrentClientQueryUsesOtherConnection()
    {
        $client = $this->client(['max_connections' => 2]);
        $thingType = 'txn_concurrent_' . uniqid();
        $parallelResult = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use ($client, $thingType, &$parallelResult) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_cost) VALUES ($1, $2)',
                [$thingType, 1.00]
            )->concat(
                $client->executeStatement("SELECT 'parallel' AS label", [])
                    ->doOnNext(function ($row) use (&$parallelResult) {
                        $parallelResult = $row['label'];
                    })
            )->concat($tx->rollback());
        })->subscribe(new CallbackObserver(
            function () {
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Concurrent query failed: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame('parallel', $parallelResult);
        $this->assertGreaterThanOrEqual(2, $client->getConnectionCount());

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testFailedTransactionRequiresRollback()
    {
        $client = $this->client();
        $error = null;
        $afterRollback = null;
        $transaction = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use (&$transaction) {
            $transaction = $tx;

            return $tx->query('SELECT 1/0');
        })->subscribe(
            function () {
                $this->fail('Expected query to fail');
            },
            function ($e) use ($client, &$error, &$afterRollback, &$transaction) {
                $error = $e;

                $transaction->rollback()->concat(
                    $client->executeStatement("SELECT 'ok' AS label", [])
                        ->doOnNext(function ($row) use (&$afterRollback) {
                            $afterRollback = $row['label'];
                        })
                )->subscribe(new CallbackObserver(
                    function () {
                        $this->stopLoop();
                    },
                    function ($e) {
                        $this->fail('Post-rollback query failed: ' . $e->getMessage());
                        $this->stopLoop();
                    }
                ));
            }
        );

        $this->runLoopWithTimeout(5);

        $this->assertNotNull($error);
        $this->assertSame('ok', $afterRollback);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testScopedTransactionAutoCommit()
    {
        $client = $this->client();
        $thingType = 'txn_scoped_commit_' . uniqid();
        $count = null;

        $client->transaction(function (Transaction $tx) use ($thingType) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_cost) VALUES ($1, $2)',
                [$thingType, 2.00]
            );
        })->flatMap(function () use ($client, $thingType) {
            return $this->countThings($client, $thingType);
        })->subscribe(new CallbackObserver(
            function ($row) use (&$count) {
                $count = (int) $row['c'];
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Count query failed: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame(1, $count);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testScopedTransactionAutoRollback()
    {
        $client = $this->client();
        $thingType = 'txn_scoped_rollback_' . uniqid();
        $count = null;
        $error = null;

        $client->transaction(function (Transaction $tx) use ($thingType) {
            return $tx->executeStatement(
                'INSERT INTO thing(thing_type, thing_cost) VALUES ($1, $2)',
                [$thingType, 2.00]
            )->concat($tx->query('SELECT 1/0'));
        })->subscribe(
            function () {
                $this->fail('Expected scoped transaction to fail');
            },
            function ($e) use ($client, $thingType, &$error, &$count) {
                $error = $e;

                $this->countThings($client, $thingType)->subscribe(new CallbackObserver(
                    function ($row) use (&$count) {
                        $count = (int) $row['c'];
                        $this->stopLoop();
                    },
                    function ($e) {
                        $this->fail('Count query failed: ' . $e->getMessage());
                        $this->stopLoop();
                    }
                ));
            }
        );

        $this->runLoopWithTimeout(5);

        $this->assertNotNull($error);
        $this->assertSame(0, $count);

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testDoubleCommitRejected()
    {
        $client = $this->client();
        $error = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) {
            return $tx->commit()->concat($tx->commit());
        })->subscribe(
            function () {
                $this->fail('Expected second commit to error');
                $this->stopLoop();
            },
            function ($e) use (&$error) {
                $error = $e;
                $this->stopLoop();
            }
        );

        $this->runLoopWithTimeout(5);

        $this->assertInstanceOf(\RuntimeException::class, $error);
        $this->assertStringContainsString('already been committed', $error->getMessage());

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testBeginTransactionWaitsForAvailableConnection()
    {
        $client = $this->client(['max_connections' => 1]);
        $firstStarted = false;
        $secondStarted = false;
        $secondBegin = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx) use ($client, &$firstStarted, &$secondBegin) {
            $firstStarted = true;
            $secondBegin = $client->beginTransaction();

            return $tx->commit();
        })->flatMap(function () use (&$secondBegin, &$secondStarted) {
            return $secondBegin->flatMap(function (Transaction $tx2) use (&$secondStarted) {
                $secondStarted = true;

                return $tx2->commit();
            });
        })->subscribe(new CallbackObserver(
            function () {
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Unexpected error: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertTrue($firstStarted);
        $this->assertTrue($secondStarted);
        $this->assertSame(1, $client->getConnectionCount());

        $client->closeNow();
        $this->getLoop()->run();
    }

    public function testQueryWaitsWhenAllConnectionsAreReservedForTransactions()
    {
        $client = $this->client(['max_connections' => 2]);
        $queryResult = null;

        $client->beginTransaction()->flatMap(function (Transaction $tx1) use ($client, &$queryResult) {
            return $client->beginTransaction()->flatMap(function (Transaction $tx2) use ($client, &$queryResult, $tx1) {
                $query = $client->executeStatement("SELECT 'queued' AS label", [])
                    ->doOnNext(function ($row) use (&$queryResult) {
                        $queryResult = $row['label'];
                    });

                return $tx2->rollback()->concat($query)->concat($tx1->rollback());
            });
        })->subscribe(new CallbackObserver(
            function () {
                $this->stopLoop();
            },
            function ($e) {
                $this->fail('Unexpected error: ' . $e->getMessage());
                $this->stopLoop();
            }
        ));

        $this->runLoopWithTimeout(5);

        $this->assertSame('queued', $queryResult);
        $this->assertSame(2, $client->getConnectionCount());

        $client->closeNow();
        $this->getLoop()->run();
    }
}
