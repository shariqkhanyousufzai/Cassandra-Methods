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
    // Three PARAM : it will select With the limit given.
    //Leave Param Empty if you dont want to use it.
    static function select($table_name,$columns = NULL,$limit = NULL){ 
        if (isset($limit) && !empty($limit)) {
            $default = $limit;
        }else{
            $default = 5000;
        }
        if (isset($columns) && !empty($columns)) {
            $sql = "SELECT ".$columns." FROM ".$table_name." LIMIT ".$default." ";
        }else{
            $sql = "SELECT * FROM ".$table_name." LIMIT ".$default."";
        }
        $statement = new Cassandra\SimpleStatement($sql);
        $result    = Cassandra_Conn::conn()->execute($statement);
        return $result;
    }
    // SELECT FUCTION END ----------


    // INSERT FUNCTION START --------------
    // FUNCTION REQUIREMENT : TABLE NAME , TABLE DATA HAVING KEYS AS COLUMNS, AND VALUE AS DATA
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
    // INSERT FUNCTION START END--------------


    // DELETE FUNCTION START --------------
    // To DELETE DATA Use This Fuction 
    // ONE PARAM : table name;
    // TWO PARAM : if columns define it will delete the specific if not it will delete all.
    // Three PARAM : You can give the condition for example id=1 and name=alex.
    static function delete($table_name,$columns = NULL,$condition = NULL){
        if (isset($condition) || !empty($condition)) {
        $sql = "DELETE ".$columns." FROM ".$table_name."";
        }else{
        $sql = "DELETE ".$columns." FROM ".$table_name." WHERE ".$condition." ";
        }
        $statement = new Cassandra\SimpleStatement($sql);
        $result    = Cassandra_Conn::conn()->execute($statement);
        return $result;
    }
    // DELETE FUNCTION START --------------


    // UPDATE FUNCTION START -------------
    static function update(){

    }
    // UPDATE FUNCTION START END -----------

    // TRUNCATE FUNCTION START --------------
    // To Empty Table Use This Fuction 
    static function truncate(){
        $sql = "TRUNCATE ".$table_name."";
        $statement = new Cassandra\SimpleStatement($sql);
        $result    = Cassandra_Conn::conn()->execute($statement);
        return $result;
    }
    // TRUNCATE FUNCTION START --------------



}
