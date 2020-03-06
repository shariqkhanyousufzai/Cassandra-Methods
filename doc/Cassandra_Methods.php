<?php
ini_set('max_execution_time', '0');
include 'Cassandra_Conn.php';

class Cassandra_Methods
  
  {
    // CASSSANDRA CONNECTION CLASS START ----------
    // Cassandra_Conn::conn() use this class to execute Statement.
    // CASSSANDRA CONNECTION CLASS END ----------
    

    // MIGRATION FROM SQL TO CASSANDRA FUCTION START ----------
    //Use this function to migrate data from MYSQL To Cassandra
    static function migration($table_name,$table_data,$datatypes){ 
        $typeCastData = json_encode($table_data);
        $convertCols = array(); 
        $convertData = array();
        $k = 0;
        foreach ($table_data as $key => $item) {
            if ($datatypes[$k] != 'varchar' && $datatypes[$k] != 'datetime' &&  $item == '')
                {$newItem =  0;}
            else if ($datatypes[$k] == 'varchar' || $datatypes[$k] == 'datetime' || $item == '')
                {$newItem =  "'".$item."'";}
            else 
                {$newItem = $item;};
            $convertData[] =  $newItem;
            $convertCols[] = ltrim($key,'_');
            $k++;
        }
        $cols = "".implode(",", array_values($convertCols))."";
        $data = "".implode(",", array_values($convertData))."";
        $table = $table_name;
        $sql = "INSERT INTO ".$table." (".$cols.") " .
                   "VALUES (".$data.")";               
        $statement = new Cassandra\SimpleStatement($sql);
        $result  = Cassandra_Conn::conn()->execute($statement);
    }
    // MIGRATION FROM SQL TO CASSANDRA FUCTION END ----------

    // SELECT FUCTION START ----------
    //Use this function to select data from Cassandra
    // ONE PARAM : it will select all if you pass one parameter
    // TWO PARAM : it will select specific columns format should be comma seprated.
    static function select($table_name,$params = NULL){ 
        if (isset($params)) {
            $sql = "SELECT ".$params." FROM ".$table_name."";
        }else{
            $sql = "SELECT * FROM ".$table_name."";
        }
        $statement = new Cassandra\SimpleStatement($sql);
        $result    = Cassandra_Conn::conn()->execute($statement);
        return $result;
    }
    // SELECT FUCTION END ----------


    // SELECT FUCTION START ----------
    //Use this function to select data from Cassandra
    // ONE PARAM : it will select all if you pass one parameter
    // TWO PARAM : it will select specific columns format should be comma seprated.
    // static function select_where($table_name,$params){ 
    //     if (isset($params)) {
    //         $sql = "SELECT ".$params." FROM ".$table_name."";
    //     }else{
    //         $sql = "SELECT * FROM ".$table_name."";
    //     }
    //     $statement = new Cassandra\SimpleStatement($sql);
    //     $result    = Cassandra_Conn::conn()->execute($statement);
    //     return $result;
    // }
    // SELECT FUCTION END ----------

    // Cassandra_Conn::conn() use this class to execute Statement.
    static function insert($table_name,$table_data){
        $typeCastData = json_encode($table_data);
        $convertCols = array(); 
        $convertData = array(); 
        foreach ($table_data as $key => $item) {
            $convertData[] =  (is_string($item) || empty($item) && !is_integer($item)) ? "'".$item."'" : "$item";
            $convertCols[] = ltrim($key,'_');
        }
        $convertDataToJson = json_decode(json_encode($convertData, JSON_NUMERIC_CHECK));
        $cols = "".implode(",", array_values($convertCols))."";
        $data = "".implode(",", array_values($convertData))."";
        $table = $table_name;
        $statement = new Cassandra\SimpleStatement(
                   "INSERT INTO ".$table." (".$cols.") " .
                   "VALUES (".$data.")"
                 );
        $result  = Cassandra_Conn::conn()->execute($statement);
        if (isset($result)) {
        return TRUE;
        }
        return FALSE;
    }



}
