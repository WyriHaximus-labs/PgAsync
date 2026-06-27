<?php
require_once __DIR__ . '/bootstrap.php';

$client = new PgAsync\Client([
    "host"     => "127.0.0.1",
    "port"     => "5432",
    "user"     => "matt",
    "database" => "matt"
]);

$client->transaction(function (PgAsync\Transaction $tx) {
    return $tx->executeStatement(
        'INSERT INTO channel(name, description) VALUES ($1, $2)',
        ['Test Name', 'Inserted inside a scoped transaction']
    )->concat(
        $tx->query("SELECT name FROM channel WHERE name = 'Test Name'")
    );
})->subscribe(
    function () {
        echo "Transaction committed.\n";
    },
    function ($e) {
        echo "Transaction failed: " . $e->getMessage() . "\n";
    }
);
