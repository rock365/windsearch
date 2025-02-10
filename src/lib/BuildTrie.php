<?php

namespace WindSearch\Core;

class BuildTrie
{
    private $trieArr = [];
    private $trieDataDir = '';

    public function __construct($fileName)
    {
        $dir =  dirname(__FILE__) . '/../windIndexCore/trieIndex/';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        $this->trieDataDir =  $dir . $fileName;
    }


    public function insert($str)
    {
        $str = trim($str);
        $str = preg_replace("/\s+|\|/", "", $str);
        $str = strtolower($str);
        // 递归插入
        $this->insert_recursion($this->trieArr, $str);
    }

    private function insert_recursion(&$res, $str)
    {
        $sub = mb_substr($str, 0, 1);
        if ($sub) {
            if (!isset($res[$sub])) {
                $res[$sub] = [];
            }
        }
        $sub2 = mb_substr($str, 1);
        if ($sub2) {
            $this->insert_recursion($res[$sub], $sub2);
        } else {
            $res[$sub]['end'] = true;
        }
        return $res;
    }

    public function storage()
    {
        return file_put_contents($this->trieDataDir, json_encode($this->trieArr));
    }

    private function check(&$trie, $str)
    {

        $sub = mb_substr($str, 0, 1);
        if ($sub) {
            if (isset($trie[$sub])) {
                // 存在，则返回true
                if (isset($trie[$sub]['end'])) {
                    return true;
                } else {
                    $sub2 = mb_substr($str, 1);
                    if ($sub2) {

                        return $this->check($trie[$sub], $sub2);
                    } else {
                        // 存在，则返回true
                        // if (isset($trie[$sub]['end'])) {

                        //     return true;
                        // } else {
                        //     return false;
                        // }
                        return false;
                    }
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }


    public function checkSensitive($word)
    {
        $this->loadData();

        $len = mb_strlen($word);
        // $cutLen = ($len >= 10) ? 10 : $len;

        // 从第一个字开始遍历
        // for ($i = 0; $i < $len; ++$i) {
        // 	for ($n = 1; $n <= ($len - $i); ++$n) {
        // 		$sub = mb_substr($word, $i, $n);
        // 		if ($obj->search($sub)) {
        // 			return true;
        // 		}else{
        // 			break;
        // 		}
        // 	}
        // }

        for ($i = 0; $i < $len; ++$i) {
            // 截取全部
            $sub = mb_substr($word, $i, $len - $i);

            if ($this->search($sub)) {
                return true;
            }
        }
        return false;
    }


    private static $allSensitiveWord = [];
    private static $tempSensitiveWord = '';

    private function checkGet(&$trie, $str)
    {


        $sub = mb_substr($str, 0, 1);

        if ($sub) {

            if (isset($trie[$sub])) {

                self::$tempSensitiveWord .= $sub;
                // 存在，则返回true
                if (isset($trie[$sub]['end'])) {
                    self::$allSensitiveWord[] =  self::$tempSensitiveWord;
                }

                $sub2 = mb_substr($str, 1);

                if ($sub2) {
                    return $this->checkGet($trie[$sub], $sub2);
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }


    /**
     * 获取字符串内所有的敏感词
     */
    public function checkSensitiveReplace($word, $replace = '**')
    {
        $this->loadData();

        $len = mb_strlen($word);

        for ($i = 0; $i < $len; ++$i) {
            // 截取全部
            $sub = mb_substr($word, $i, $len - $i);
            $this->search_replace($sub);
            self::$tempSensitiveWord = '';
        }
        if (!empty(self::$allSensitiveWord)) {
            $len = count(self::$allSensitiveWord);
            $replace_str = $replace;
            $comb_replace_str = array_pad([], $len, $replace_str);

            $comb = array_combine(self::$allSensitiveWord, $comb_replace_str);
            return strtr($word, $comb);
        }
    }

    /**
     * 获取所有敏感词
     */
    public function getAllSensitiveWords($word)
    {
       
        $this->loadData();

        $len = mb_strlen($word);

        for ($i = 0; $i < $len; ++$i) {
            // 截取全部
            $sub = mb_substr($word, $i, $len - $i);
            $this->search_replace($sub);
            self::$tempSensitiveWord = '';
        }
        return self::$allSensitiveWord;
    }



    private function loadData()
    {
        if (empty($this->trieArr)) {
            if (is_file($this->trieDataDir)) {
                $this->trieArr = (array)json_decode(file_get_contents($this->trieDataDir), true);
            }
        }
    }


    /**
     * 搜索是否存在敏感词
     */
    public function search($word)
    {
        return $this->check($this->trieArr, $word);
    }

    /**
     * 搜索替换敏感词
     */
    public function search_replace($word)
    {
        return $this->checkGet($this->trieArr, $word);
    }
}
