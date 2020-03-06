<?php

class Cassandra_Conn
{

	// CASSSANDRA CONNECTION CLASS START ----------
	static function conn()
    {
        $cluster   = Cassandra::cluster()
               ->withContactPoints('127.0.0.1')
               ->build();
        $session = $cluster->connect("voltswitch");
        return $session;
    }
    // CASSSANDRA CONNECTION CLASS END ----------
}