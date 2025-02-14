<?php

namespace WindSearch\Core;

use WindSearch\DAO\PDO_sqlite;

class Cache
{

    /**
     * 缓存文件初始化
     */
    public function initCache()
    {

        $currDir = dirname(__FILE__);


        // sqlite3存储
        // 使用sqlite存储倒排索引

        $toDir = $currDir . '/../cache/';
        if (!is_dir($toDir)) {
            mkdir($toDir, 0777);
        }

        $dir = $currDir . '/../cache/cache.db';

        if (is_file($dir)) {
            return;
        }
        $pdo = new PDO_sqlite($dir);

        // 创建表
        $pdo->exec("CREATE TABLE IF NOT EXISTS cache (
			id INTEGER PRIMARY KEY,
			search_key TEXT,
			content TEXT)");
        // 对term字段创建唯一索引
        $sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_cache ON cache(search_key);";
        $pdo->exec($sql);
    }


    /**
     * 设置搜索结果缓存
     * @param $timeOut 缓存过期时间 秒
     */
    public function setCache($key, $values, $cachetimeout, $indexname, $redisObj = false)
    {
        if (is_object($redisObj)) {
            $key = $indexname . '_' . $key;
            $values = base64_encode(gzdeflate($values));
            $redisObj->setex($key, $cachetimeout, $values);
        } else {
           
            $this->initCache();

            if ($key) {

                $key = $indexname . '_' . $key;

                $currDir = dirname(__FILE__);
                $dir = $currDir . '/../cache/cache.db';

                $pdo = new PDO_sqlite($dir);

                $values = [
                    'settime' => time(),
                    'timeout' => $cachetimeout,
                    'content' => $values,
                ];
               
                $values = base64_encode(gzdeflate(json_encode($values)));
               
                $sql = "insert into cache (search_key,content)values('$key','$values');";
                $pdo->exec($sql);
            }
        }
    }


    /**
     * 获取缓存
     */
    public function getCache($key, $indexname, $redisObj = false)
    {
        if (is_object($redisObj)) {
            $key = $indexname . '_' . $key;
            $content = $redisObj->get($key);
            $res = json_decode(gzinflate(base64_decode($content)), true);
            return $res;
        } else {

            $this->initCache();

            if ($key) {

                $key = $indexname . '_' . $key;

                $currDir = dirname(__FILE__);
                $dir = $currDir . '/../cache/cache.db';

                $dbname = $dir;
                $pdo = new PDO_sqlite($dbname);
                

                $sql = "select id,content from cache where search_key='$key';";
                $res = $pdo->getRow($sql);

                if ($res) {
                    $content = isset($res['content']) ? $res['content'] : '';
                    $id = isset($res['id']) ? $res['id'] : '';
                   
                    if ($content != '') {
                        $res = json_decode(gzinflate(base64_decode($content)), true);
                        
                        $timeout = $res['timeout'];
                        $content = $res['content'];
                        $settime = $res['settime'];
                       
                        // 判断过期
                        if ((int)$timeout > 0) {
                            // 过期
                            if ((time() - $settime) > $timeout) {
                                // 删除过期缓存
                                $sql = "delete from cache where id='$id';";
                                $pdo->exec($sql);
                                $content = false;
                            }
                        }
                    }
                } else {
                    $content = false;
                }

                
                return $content;
            } else {
                return false;
            }
        }
    }


    /**
     * 删除整个缓存文件夹
     */
    public function delCache()
    {
        // $currDir = dirname(__FILE__);
        // $dir = $currDir . '/../cache/cache.db';
        // $dbname = $dir;
        // $pdo = new PDO_sqlite($dbname);

        // $sql = "DROP TABLE IF EXISTS cache;";
        // $pdo->exec($sql);

        $currDir = dirname(__FILE__);
        $dir = $currDir . '/../cache/';

        $this->del_dir($dir);
    }


    /**
     * 删除文件夹及其文件夹下所有文件
     * @param $dir 要删除的文件夹路径
     */
    private static function del_dir($dir)
    {
        if (substr($dir, -1) != '/') {
            $dir = $dir . '/';
        }
        if (!is_dir($dir)) {
            return;
        }

        $fileList = scandir($dir);

        foreach ($fileList as $file) {

            if (($file != ".") && ($file != "..")) {
                $fullpath = $dir . $file;

                if (!is_dir($fullpath)) {

                    unlink($fullpath);
                } else {
                    self::del_chilren_dir($fullpath);
                }
            }
        }
        //删除当前文件夹
        rmdir($dir);
    }


    private static function del_chilren_dir($dir)
    {
        $fileList = scandir($dir);
        foreach ($fileList as $file) {

            if (($file != ".") && ($file != "..")) {
                $fullpath = $dir . '/' . $file;

                if (!is_dir($fullpath)) {
                    unlink($fullpath);
                } else {
                    self::del_chilren_dir($fullpath);
                }
            }
        }
        //删除当前文件夹：
        rmdir($dir);
    }
}
