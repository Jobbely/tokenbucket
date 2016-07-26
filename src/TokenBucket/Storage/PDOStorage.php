<?php

namespace TokenBucket\Storage;

use PDO;
use TokenBucket\Exception\StorageException;

/**
 * Getters and Setters to a given table on a given pdo connection
 *
 * @todo exceptions && error handling (eg of un-serialisation)
 */
class PDOStorage implements StorageInterface
{
	protected $pdo;
	protected $tableName;

	public function __construct(PDO $pdo, $tableName = "TokenBucket")
	{
		$this->pdo = $pdo;
		$this->tableName = $tableName;
	}

	public function getStorageName()
	{
		return "PDO";
	}

    /**
     * Gets a value that belongs to a given $key.
     *
     * @param string $key
     *
     * @return mixed
     */
    public function get($key)
    {
    	$statement = $this->pdo->prepare( "SELECT value FROM {$this->tableName} WHERE name=?" );
    	$statement->execute( [$key]);

    	$value = $statement->fetchColumn();

    	$statement->closeCursor();

    	return unserialize($value);
    }

    /**
     * Sets a given $value in a container given by $key.
     *
     * @param string $key
     * @param mixed  $value
     *
     * @return void
     */
    public function set($key, $value)
    {
    	$insert = $this->pdo->prepare(
            "INSERT INTO {$this->tableName} (name, value) VALUES (:key, :value) ON DUPLICATE KEY UPDATE value = :value"
        );
        $insert->execute(['key' => $key, 'value' => serialize($value)]);
    }

    /**
     * Deletes an entry from storage.
     *
     * @param string $key
     *
     * @return void
     */
    public function delete($key)
    {
    	$delete = $this->pdo->prepare("DELETE FROM {$this->tableName} WHERE name = ?");
        $delete->execute([$key]);
    }

    /**
     * Get create table sql. Only needed to execute once. (Here for convenience :-)
     * @param  string $storage_engine Optionally add a used storage engine, such as InnoDB. For speed, using MEMORY by default
     * @return string                 The SQL
     */
    public function getCreateTableSQL($storage_engine = 'MEMORY')
    {
    	return "CREATE TABLE {$this->tableName} (
                        name  VARCHAR(128) PRIMARY KEY,
                        value VARCHAR(255) NOT NULL
                     ) ENGINE=$storage_engine;";
    }
}