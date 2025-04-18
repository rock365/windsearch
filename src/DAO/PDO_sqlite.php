<?php

namespace WindSearch\DAO;

use WindSearch\Exceptions\WindException;

// sqlite类
class PDO_sqlite
{

  // sqlite对象
  private $_obj;

  /**
   * 构造函数
   */
  public function __construct($dbname)
  {
    // throw new WindException('1111111', 0);
    // 数据库名称 带路径
    if (!$dbname) {
      $msg = 'sqlite无配置信息';
      $code = 0;
      throw new WindException($msg, $code);
    }
    try {
      // 连接数据库
      $this->_obj = new \PDO('sqlite:' . $dbname);
    } catch (\PDOException $e) {
      $msg = 'PHP的pdo_sqlite扩展未开启';
      $code = 0;
      throw new WindException($msg, $code);
    }
  }

  /**
   * 初始化各种表
   */
  public function init($IndexName, $primarykey, $primarykeyType)
  {
    // 删除表的SQL语句
    $dropTableSql = "DROP TABLE IF EXISTS $IndexName";
    // 执行SQL语句删除表
    $this->_obj->exec($dropTableSql);

    // 主键类型为整型递增，则索引存储结构为bitmap跟postlist
    if ($primarykeyType == 'Int_Incremental') {
      // 创建表
      // wind_sys_id 系统自增id，强制
      // $primarykey 用户自定义的主键 非强制
      $res = $this->_obj->exec("CREATE TABLE IF NOT EXISTS $IndexName (
		           wind_sys_id INTEGER PRIMARY KEY,
               $primarykey INTEGER,
		           doc TEXT
		           )");
      // 对用户int主键字段创建唯一索引
      $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $primarykey . " ON " . $IndexName . "($primarykey);";
      $this->_obj->exec($sql_index);
    } else {
      // 创建表
      $res = $this->_obj->exec("CREATE TABLE IF NOT EXISTS $IndexName (
      wind_sys_id INTEGER PRIMARY KEY,
      $primarykey TEXT,
      doc TEXT
      )");
      // 对用户UUID主键字段创建唯一索引
      $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $primarykey . " ON " . $IndexName . "($primarykey);";
      $this->_obj->exec($sql_index);
    }

    if ((int)$res !== 0) {
      $msg = 'sqlite创建基础数据表失败（请检查<自定义主键>的名称是否为系统关键字，或进行其它检查）';
      $code = 0;
      throw new WindException($msg, $code);
    }
  }

  /**
   * 开始事务
   */
  public function beginTransaction()
  {
    $this->_obj->beginTransaction();
  }

  /**
   * 提交事务
   */
  public function commit()
  {
    $this->_obj->commit();
  }

  /**
   * 运行语句 无数据返回
   */
  public function exec($query)
  {
    $result = $this->_obj->exec($query);
    return $result;
  }

  /**
   * 查询单行数据
   */
  public function getRow($query)
  {
    $resRow = false;
    $res = $this->_obj->query($query);
    if ($res) {
      $resRow = $res->fetch(\PDO::FETCH_ASSOC);
    }
    return $resRow;
  }


  /**
   * 查询多行数据
   */
  public function getAll($query)
  {
    $resList = false;
    $res = $this->_obj->query($query);

    if ($res) {
      $resList = $res->fetchAll(\PDO::FETCH_ASSOC);
    }
    return $resList;
  }
}
