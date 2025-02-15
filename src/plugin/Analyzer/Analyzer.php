<?php

// +----------------------------------------------------------------------
// | rockAnalyzer
// +----------------------------------------------------------------------
// | Copyright (c) All rights reserved.
// +----------------------------------------------------------------------
// | Licensed: Apache License 2.0
// +----------------------------------------------------------------------
// | Author: rock365
// +----------------------------------------------------------------------
// | PHP Version: PHP_VERSION >= 5.6
// +----------------------------------------------------------------------

namespace WindSearch\Plugin;


class Analyzer
{
    //hash算法选项 词库很大时可以选择更大的数值 防止单个哈希下面有过多的数组
    private $mask_value = 0xc3500; //0x7a120; //0xFFFF; // 十进制是 65535    切换16进制 0x7a120 10进制是500000  0xc3500 10进制是800000

    private $cache = [];
    private $resultArr = [];


    private $mainDicHand = false;
    private $mainDicInfos = [];
    private $mainDicFile = '';

    private $stem;
    private $dicMap = [];
    private $isDicMap = false;
    private $customWord = [];
    private $notWord = [];

    /**
     * 构造函数
     * @param $source_charset
     * @param $target_charset
     * @param $load_alldic 
     * @param $source
     *
     * @return void
     */
    public function __construct()
    {

        // 基本词库
        $this->mainDicFile = dirname(__FILE__) . '/dic/dic_with_idf.dic';

        // 自定义词库
        $customWord = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/custom_dic.txt'));
        $this->customWord = array_flip((array)$customWord);

        // 自定义非词
        $notWord = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/dic/not_word.txt'));
        $this->notWord = array_flip((array)$notWord);





        // 英文单词转词根
        // require './PorterStemmer.php';
        // $this->stem = new \PorterStemmer();
    }

    /**
     * 析构函数
     */
    function __destruct()
    {
        if ($this->mainDicHand !== false) {
            @fclose($this->mainDicHand);
        }
        unset($this->resultArr);
        unset($this->cache);
    }

    public function loadDicMap()
    {
        $this->dicMap = json_decode(file_get_contents(dirname(__FILE__) . '/dic/map_dic.dic'), true);

        foreach ($this->customWord as $k => $v) {
            $this->dicMap[$k] = '';
        }

        foreach ($this->notWord as $k => $v) {
            unset($this->dicMap[$k]);
        }


        $this->isDicMap = true;
    }

    public function destroyDic()
    {
        unset($this->dicMap);
    }
    // public function addDicIconv()
    // {
    //     $tempDic = [];
    //     foreach ($this->customWord as $k => $v) {
    //         $k = iconv('utf-8', 'ucs-2be', $k);
    //         $tempDic[$k] = '';
    //     }
    //     $this->customWord = $tempDic;

    //     $tempDic = [];
    //     foreach ($this->notWord as $k => $v) {
    //         $k = iconv('utf-8', 'ucs-2be', $k);
    //         $tempDic[$k] = '';
    //     }
    //     $this->notWord = $tempDic;
    // }

    /**
     * 根据字符串计算key索引
     * @param $key
     * @return short int
     */
    private function _get_index($key)
    {
        $l = strlen($key);
        $h = 0x238f13af;
        while ($l--) {
            $h += ($h << 5);
            $h ^= ord($key[$l]);
            $h &= 0x7fffffff;
        }


        return ($h % $this->mask_value);
    }

    /**
     * 从文件获得词
     * @param $key
     * @param $type (类型 word 或 key_groups)
     * @return short int
     */
    public function GetWordInfos($key, $type = 'word')
    {


        if (!$this->mainDicHand) {
            $this->mainDicHand = fopen($this->mainDicFile, 'r');
        }


        $keynum = (int)$this->_get_index($key);

        //缓存
        if (isset($this->mainDicInfos[$keynum])) {
            $data = $this->mainDicInfos[$keynum];
        } else {
            //rewind( $this->mainDicHand );

            //词库1 较快
            $move_pos = $keynum * 12;
            fseek($this->mainDicHand, $move_pos, SEEK_SET);
            $dat = fread($this->mainDicHand, 12);
            $arr = unpack('I1s/N1l/N1c', $dat);

            if ($arr['l'] == 0) {
                return false;
            }
            fseek($this->mainDicHand, $arr['s'], SEEK_SET);
            $data = json_decode(fread($this->mainDicHand, $arr['l']), true);

            $this->mainDicInfos[$keynum] = $data;
        }


        if (!is_array($data) || !isset($data[$key])) {
            return false;
        }

        return (($type == 'word') ? $data[$key] : $data);
        // return ($type=='word' ? $data : $data);

    }


    /**
     * 检测某个词是否存在
     */
    public function IsWord($word)
    {



        if (isset($this->notWord[$word])) {
            return false;
        }

        if (isset($this->customWord[$word])) {
            return true;
        }

        $winfos = $this->GetWordInfos($word);

        return ($winfos !== false);
    }



    public function getTerm($zfc)
    {


        // 最大比对长度
        $max_len = 6;
        $fcContainerUtf8 = [];

        $strLen = strlen($zfc);

        if ($strLen <= $max_len * 3) {

            $max_len = (($strLen / 3));
        }

        if ($strLen == 3) {

            $fcContainerUtf8[] = $zfc;
        }

        $tempArr = [
            '着' => '',
            '上' => '',
            '下' => '',
            '中' => '',
            '里' => '',
            '外' => '',
            '出' => ''
        ];

        // 初始截取位置
        $currCutPos = $strLen - 3 * $max_len;
        $tempCount = 0;
        // 如果剩一个字，无法循环处理，必须单独放入结果集保存
        for ($g = $currCutPos; $g < ($strLen - 3); $g += 3) {

            ++$tempCount;
            $isExitTerm = false;
            $str_cut = substr($zfc, $g);


            if (!$this->isDicMap) {

                // $str_cut_iconv = iconv('utf-8', 'ucs-2be', $str_cut);
                $is_wd = $this->IsWord($str_cut);
            } else {

                $is_wd = isset($this->dicMap[$str_cut]);
            }

            // 是一个词语
            if ($is_wd) {
                // 是一个词语 true
                $isExitTerm = true;
                // ucs-2be 分词保存
                // $fcContainerUcs[] = $str_cut_iconv;
                // utf8 分词保存

                // 判别1 概率基础：最后两个字与邻近的几个字都分别独立属于一个词，这种情况下，最后两个字成的词，大概率在当前语义中，就是一个独立的词，特殊字结尾除外（上、下、...）
                // 非进阶截取模式，截取到了最后两个字，并且最后两个字是一个词语，注意，基础字符串至少有4个字，才有意义 3*4=12

                // 截取到最后两个字
                if (($tempCount == ($max_len - 1))) {
                    // 最后一个字
                    $lastWord = substr($str_cut, 3);


                    // if (!in_array($lastWord, $tempArr)) {
                    //     // 倒查邻近的3个字
                    //     $beg = ($g - 9);
                    //     // >=6 说明可以倒查邻近2个或3个字
                    //     // 如果邻近的3个或两个字为一个词，则最后两个字就无需进行后面的判别
                    //     if ($beg >= 0) {

                    //         $tempWord = substr($zfc, ($g - 9), 9);

                    //         $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //         $is_wd = $this->IsWord($tempWord_iconv);
                    //         if ($is_wd) {
                    //             // 目前最后两个字
                    //             $fcContainerUtf8[] = $str_cut;
                    //             // 邻近的两个字 不保存
                    //             // $fcContainerUtf8[] =  $tempWord;

                    //             // 重新赋值字符串
                    //             $zfc = substr($zfc, 0, $g);

                    //             $strLen = strlen($zfc);
                    //             if ($strLen <= $max_len * 3) {
                    //                 $max_len = ($strLen / 3);
                    //             }
                    //             // 还剩最后一个字
                    //             if ($strLen == 3) {
                    //                 $fcContainerUtf8[] = $zfc;
                    //                 break;
                    //             }
                    //             // 初始截取位置
                    //             $g = $strLen - 3 * $max_len - 3;

                    //             $tempCount = 0;
                    //             $isExitTerm = false;
                    //             // 重新循环比对，跳过下面的代码
                    //             continue;
                    //         } else {
                    //             $tempWord = substr($zfc, ($g - 6), 6);

                    //             $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //             $is_wd = $this->IsWord($tempWord_iconv);
                    //             if ($is_wd) {
                    //                 // 目前最后两个字
                    //                 $fcContainerUtf8[] = $str_cut;
                    //                 // 邻近的两个字
                    //                 // $fcContainerUtf8[] =  $tempWord;

                    //                 // 重新赋值字符串
                    //                 $zfc = substr($zfc, 0, $g);

                    //                 $strLen = strlen($zfc);
                    //                 if ($strLen <= $max_len * 3) {
                    //                     $max_len = ($strLen / 3);
                    //                 }
                    //                 // 还剩最后一个字
                    //                 if ($strLen == 3) {
                    //                     $fcContainerUtf8[] = $zfc;
                    //                     break;
                    //                 }
                    //                 // 初始截取位置
                    //                 $g = $strLen - 3 * $max_len - 3;

                    //                 $tempCount = 0;
                    //                 $isExitTerm = false;
                    //                 // 重新循环比对，跳过下面的代码
                    //                 continue;
                    //             }
                    //         }
                    //     } else {

                    //         $beg = ($g - 6);
                    //         // >=6 说明可以倒查邻近2个字
                    //         if ($beg >= 0) {

                    //             $tempWord = substr($zfc, ($g - 6), 6);

                    //             $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //             $is_wd = $this->IsWord($tempWord_iconv);
                    //             if ($is_wd) {
                    //                 // 目前最后两个字
                    //                 $fcContainerUtf8[] = $str_cut;
                    //                 // 邻近的两个字
                    //                 // $fcContainerUtf8[] =  $tempWord;

                    //                 // 重新赋值字符串
                    //                 $zfc = substr($zfc, 0, $g);

                    //                 $strLen = strlen($zfc);
                    //                 if ($strLen <= $max_len * 3) {
                    //                     $max_len = ($strLen / 3);
                    //                 }
                    //                 // 还剩最后一个字
                    //                 if ($strLen == 3) {
                    //                     $fcContainerUtf8[] = $zfc;
                    //                     break;
                    //                 }
                    //                 // 初始截取位置
                    //                 $g = $strLen - 3 * $max_len - 3;

                    //                 $tempCount = 0;
                    //                 $isExitTerm = false;
                    //                 // 重新循环比对，跳过下面的代码
                    //                 continue;
                    //             }
                    //         }
                    //     }
                    // }
                    if (isset($tempArr[$lastWord])) {

                        // if (in_array($lastWord, $tempArr)) {
                        // 如果上面的符合任何一种情况，则不会运行到这里来
                        // 倒查三个字
                        $beg = ($g - 6);
                        // >=0，说明可倒查2个或3个
                        if ($beg >= 0) {

                            $tempWord = substr($zfc, ($g - 6), 9);


                            if (!$this->isDicMap) {

                                // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                $is_wd = $this->IsWord($tempWord);
                            } else {

                                $is_wd = isset($this->dicMap[$tempWord]);
                            }


                            if ($is_wd) {
                                $str_cut_last = substr($zfc, -3);

                                $fcContainerUtf8[] = $str_cut_last;
                                $fcContainerUtf8[] =  $tempWord;

                                // 重新赋值字符串
                                $zfc = substr($zfc, 0, ($g - 6));

                                $strLen = strlen($zfc);
                                if ($strLen <= $max_len * 3) {
                                    $max_len = ($strLen / 3);
                                }
                                // 还剩最后一个字
                                if ($strLen == 3) {
                                    $fcContainerUtf8[] = $zfc;
                                    break;
                                }
                                // 初始截取位置
                                $g = $strLen - 3 * $max_len - 3;

                                $tempCount = 0;
                                $isExitTerm = false;
                                // 重新循环比对，跳过下面的代码
                                continue;
                            }
                            // 倒查两个字
                            else {
                                $tempWord = substr($zfc, ($g - 3), 6);



                                if (!$this->isDicMap) {
                                    // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                    $is_wd = $this->IsWord($tempWord);
                                } else {
                                    $is_wd = isset($this->dicMap[$tempWord]);
                                }



                                if ($is_wd) {
                                    $str_cut_last = substr($zfc, -3);

                                    $fcContainerUtf8[] = $str_cut_last;
                                    $fcContainerUtf8[] = $tempWord;

                                    // 重新赋值字符串
                                    $zfc = substr($zfc, 0, ($g - 3));

                                    $strLen = strlen($zfc);
                                    if ($strLen <= $max_len * 3) {
                                        $max_len = ($strLen / 3);
                                    }
                                    // 还剩最后一个字
                                    if ($strLen == 3) {

                                        $fcContainerUtf8[] = $zfc;
                                        break;
                                    }
                                    // 初始截取位置
                                    $g = $strLen - 3 * $max_len - 3;

                                    $tempCount = 0;
                                    $isExitTerm = false;
                                    // 重新循环比对，跳过下面的代码
                                    continue;
                                }
                            }
                        }
                        // 只能倒查两个字
                        else {
                            $beg = ($g - 3);

                            // 只能倒查两个字
                            if ($beg >= 0) {


                                $tempWord = substr($zfc, ($g - 3), 6);


                                if (!$this->isDicMap) {
                                    // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                    $is_wd = $this->IsWord($tempWord);
                                } else {
                                    $is_wd = isset($this->dicMap[$tempWord]);
                                }



                                if ($is_wd) {
                                    $str_cut_last = substr($zfc, -3);

                                    $fcContainerUtf8[] = $str_cut_last;
                                    $fcContainerUtf8[] = $tempWord;

                                    // 重新赋值字符串
                                    $zfc = substr($zfc, 0, ($g - 3));

                                    $strLen = strlen($zfc);
                                    if ($strLen <= $max_len * 3) {
                                        $max_len = ($strLen / 3);
                                    }
                                    // 还剩最后一个字
                                    if ($strLen == 3) {

                                        $fcContainerUtf8[] = $zfc;
                                        break;
                                    }
                                    // 初始截取位置
                                    $g = $strLen - 3 * $max_len - 3;

                                    $tempCount = 0;
                                    $isExitTerm = false;
                                    // 重新循环比对，跳过下面的代码
                                    continue;
                                }
                            }
                        }
                    }
                }

                // 判别1 成功，则不会运行到这里，不成功，则会运行到这里
                $fcContainerUtf8[] = $str_cut;



                // 截取后，更新字符串
                $zfc = substr($zfc, 0, $g);
                $strLen = strlen($zfc);
                if ($strLen <= $max_len * 3) {
                    $max_len = ($strLen / 3);
                }
                // 下一次初始截取位置
                $g = $strLen - 3 * $max_len - 3;

                $tempCount = 0;
                $isExitTerm = false;
            }

            // 截取到了最后两个字，并且没有比对出任何词语
            if (($tempCount == ($max_len - 1)) && !$isExitTerm) {

                $strLen = strlen($zfc);
                // 剩最后一个字符 直接保存为一个词
                if ($strLen == 3) {
                    $fcContainerUtf8[] = $zfc;

                    // $str_cut_last_iconv = iconv('utf-8', 'ucs-2be', $zfc);
                    // $fcContainerUcs[] = $str_cut_last_iconv;
                    break;
                }
                // 剩余的字符串还很长，将最后一个字切出来，作为一个单独的词
                else {
                    $str_cut_last = substr($zfc, -3);
                    $fcContainerUtf8[] = $str_cut_last;


                    // $str_cut_last_iconv = iconv('utf-8', 'ucs-2be', $str_cut_last);
                    // $fcContainerUcs[] = $str_cut_last_iconv;

                    // 更新字符串
                    $zfc = substr($zfc, 0, -3);
                    $strLen = strlen($zfc);
                    // 剩余字符个数少于等于7个时，字符个数就为最大切分长度
                    if ($strLen <= $max_len * 3) {
                        $max_len = ($strLen / 3);
                    }
                    // 下一次初始截取位置
                    $g = $strLen - 3 * $max_len - 3;
                }

                // 最后剩一个字，无法循环处理，单独放入结果集保存
                if ($strLen == 3) {
                    $fcContainerUtf8[] = $zfc;
                    break;
                }

                $tempCount = 0;
                $isExitTerm = false;
            }
        }

        // //数组上下翻转，调整顺序
        $fcContainerUtf8 = array_reverse($fcContainerUtf8);

        return $fcContainerUtf8;
    }

    public function getAllTerm($zfc, $type = false)
    {


        // 转为utf8编码
        // $zfc = iconv('ucs-2be', 'utf-8', $zfc);

        // 最大比对长度
        $max_len = 6;
        $fcContainerUcs = [];
        $fcContainerUtf8 = [];

        $strLen = strlen($zfc);

        if ($strLen <= $max_len * 3) {
            // 长词进一步分词
            if ($type) {
                // -1,是为了防止函数无限嵌套
                $max_len = (($strLen / 3) - 1);
            }
            // 普通分词
            else {
                $max_len = (($strLen / 3));
            }
        }

        if ($strLen == 3) {

            $fcContainerUtf8[] = $zfc;
        }

        $tempArr = [
            '着' => '',
            '上' => '',
            '下' => '',
            '中' => '',
            '里' => '',
            '外' => '',
            '出' => ''
        ];

        // 初始截取位置
        $currCutPos = $strLen - 3 * $max_len;
        $tempCount = 0;
        // 如果剩一个字，无法循环处理，必须单独放入结果集保存
        for ($g = $currCutPos; $g < ($strLen - 3); $g += 3) {

            ++$tempCount;
            $isExitTerm = false;
            $str_cut = substr($zfc, $g);

            if (!$this->isDicMap) {
                // $str_cut_iconv = iconv('utf-8', 'ucs-2be', $str_cut);
                $is_wd = $this->IsWord($str_cut);
            } else {

                $is_wd = isset($this->dicMap[$str_cut]);
            }

            // 是一个词语
            if ($is_wd) {

                $isExitTerm = true;
                // ucs-2be 分词保存
                // $fcContainerUcs[] = $str_cut_iconv;
                // utf8 分词保存

                // 判别1
                // 非进阶截取模式，截取到了最后两个字，并且最后两个字是一个词语，注意，基础字符串至少有4个字，才有意义 3*4=12
                if (($tempCount == ($max_len - 1)) && !$type) {
                    $lastWord = substr($str_cut, 3);


                    // if (!in_array($lastWord, $tempArr)) {
                    //     // 倒查邻近的3个字
                    //     $beg = ($g - 9);
                    //     // >=6 说明可以倒查邻近2个或3个字
                    //     // 如果邻近的3个或两个字为一个词，则最后两个字就无需进行后面的判别
                    //     if ($beg >= 0) {

                    //         $tempWord = substr($zfc, ($g - 9), 9);

                    //         $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //         $is_wd = $this->IsWord($tempWord_iconv);
                    //         if ($is_wd) {
                    //             // 目前最后两个字
                    //             $fcContainerUtf8[] = $str_cut;
                    //             // 邻近的两个字 不保存
                    //             // $fcContainerUtf8[] =  $tempWord;

                    //             // 重新赋值字符串
                    //             $zfc = substr($zfc, 0, $g);

                    //             $strLen = strlen($zfc);
                    //             if ($strLen <= $max_len * 3) {
                    //                 $max_len = ($strLen / 3);
                    //             }
                    //             // 还剩最后一个字
                    //             if ($strLen == 3) {
                    //                 $fcContainerUtf8[] = $zfc;
                    //                 break;
                    //             }
                    //             // 初始截取位置
                    //             $g = $strLen - 3 * $max_len - 3;

                    //             $tempCount = 0;
                    //             $isExitTerm = false;
                    //             // 重新循环比对，跳过下面的代码
                    //             continue;
                    //         } else {
                    //             $tempWord = substr($zfc, ($g - 6), 6);

                    //             $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //             $is_wd = $this->IsWord($tempWord_iconv);
                    //             if ($is_wd) {
                    //                 // 目前最后两个字
                    //                 $fcContainerUtf8[] = $str_cut;
                    //                 // 邻近的两个字
                    //                 // $fcContainerUtf8[] =  $tempWord;

                    //                 // 重新赋值字符串
                    //                 $zfc = substr($zfc, 0, $g);

                    //                 $strLen = strlen($zfc);
                    //                 if ($strLen <= $max_len * 3) {
                    //                     $max_len = ($strLen / 3);
                    //                 }
                    //                 // 还剩最后一个字
                    //                 if ($strLen == 3) {
                    //                     $fcContainerUtf8[] = $zfc;
                    //                     break;
                    //                 }
                    //                 // 初始截取位置
                    //                 $g = $strLen - 3 * $max_len - 3;

                    //                 $tempCount = 0;
                    //                 $isExitTerm = false;
                    //                 // 重新循环比对，跳过下面的代码
                    //                 continue;
                    //             }
                    //         }
                    //     } else {

                    //         $beg = ($g - 6);
                    //         // >=6 说明可以倒查邻近2个字
                    //         if ($beg >= 0) {

                    //             $tempWord = substr($zfc, ($g - 6), 6);

                    //             $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                    //             $is_wd = $this->IsWord($tempWord_iconv);
                    //             if ($is_wd) {
                    //                 // 目前最后两个字
                    //                 $fcContainerUtf8[] = $str_cut;
                    //                 // 邻近的两个字
                    //                 // $fcContainerUtf8[] =  $tempWord;

                    //                 // 重新赋值字符串
                    //                 $zfc = substr($zfc, 0, $g);

                    //                 $strLen = strlen($zfc);
                    //                 if ($strLen <= $max_len * 3) {
                    //                     $max_len = ($strLen / 3);
                    //                 }
                    //                 // 还剩最后一个字
                    //                 if ($strLen == 3) {
                    //                     $fcContainerUtf8[] = $zfc;
                    //                     break;
                    //                 }
                    //                 // 初始截取位置
                    //                 $g = $strLen - 3 * $max_len - 3;

                    //                 $tempCount = 0;
                    //                 $isExitTerm = false;
                    //                 // 重新循环比对，跳过下面的代码
                    //                 continue;
                    //             }
                    //         }
                    //     }
                    // }
                    if (isset($tempArr[$lastWord])) {
                        // if (in_array($lastWord, $tempArr)) {
                        // 如果上面的符合任何一种情况，则不会运行到这里来
                        // 倒查三个字
                        $beg = ($g - 6);
                        // >=0，说明可倒查2个或3个
                        if ($beg >= 0) {
                            // 倒查三个字
                            $tempWord = substr($zfc, ($g - 6), 9);

                            if (!$this->isDicMap) {
                                // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                $is_wd = $this->IsWord($tempWord);
                            } else {
                                $is_wd = isset($this->dicMap[$tempWord]);
                            }



                            if ($is_wd) {
                                $str_cut_last = substr($zfc, -3);

                                $fcContainerUtf8[] = $str_cut_last;

                                // 长词再一次分词
                                // $tempRe = $this->getAllTerm($tempWord, true);
                                $tempRe = $this->deepFc($tempWord);
                                // $fcContainerUcs = array_merge($fcContainerUcs, $tempRe[0]);
                                $tempRe[1][] = $tempWord;
                                // $fcContainerUtf8 = array_merge($fcContainerUtf8, $tempRe[1]);
                                foreach ($tempRe[1] as $w) {
                                    $fcContainerUtf8[] = $w;
                                }


                                // 重新赋值字符串
                                $zfc = substr($zfc, 0, ($g - 6));

                                $strLen = strlen($zfc);
                                if ($strLen <= $max_len * 3) {
                                    $max_len = ($strLen / 3);
                                }
                                // 还剩最后一个字
                                if ($strLen == 3) {

                                    $fcContainerUtf8[] = $zfc;
                                    break;
                                }
                                // 初始截取位置
                                $g = $strLen - 3 * $max_len - 3;

                                $tempCount = 0;
                                $isExitTerm = false;
                                // 重新循环比对，跳过下面的代码
                                continue;
                            }

                            // 倒查两个字
                            else {
                                $tempWord = substr($zfc, ($g - 3), 6);

                                if (!$this->isDicMap) {
                                    // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                    $is_wd = $this->IsWord($tempWord);
                                } else {
                                    $is_wd = isset($this->dicMap[$tempWord]);
                                }



                                if ($is_wd) {
                                    $str_cut_last = substr($zfc, -3);

                                    $fcContainerUtf8[] = $str_cut_last;
                                    $fcContainerUtf8[] = $tempWord;

                                    // 重新赋值字符串
                                    $zfc = substr($zfc, 0, ($g - 3));

                                    $strLen = strlen($zfc);
                                    if ($strLen <= $max_len * 3) {
                                        $max_len = ($strLen / 3);
                                    }
                                    // 还剩最后一个字
                                    if ($strLen == 3) {

                                        $fcContainerUtf8[] = $zfc;
                                        break;
                                    }
                                    // 初始截取位置
                                    $g = $strLen - 3 * $max_len - 3;

                                    $tempCount = 0;
                                    $isExitTerm = false;
                                    // 重新循环比对，跳过下面的代码
                                    continue;
                                }
                            }
                        } else {
                            $beg = ($g - 3);
                            // 只能倒查两个字
                            if ($beg >= 0) {

                                $tempWord = substr($zfc, ($g - 3), 6);

                                if (!$this->isDicMap) {
                                    // $tempWord_iconv = iconv('utf-8', 'ucs-2be', $tempWord);
                                    $is_wd = $this->IsWord($tempWord);
                                } else {
                                    $is_wd = isset($this->dicMap[$tempWord]);
                                }



                                if ($is_wd) {
                                    $str_cut_last = substr($zfc, -3);

                                    $fcContainerUtf8[] = $str_cut_last;
                                    $fcContainerUtf8[] = $tempWord;

                                    // 重新赋值字符串
                                    $zfc = substr($zfc, 0, ($g - 3));

                                    $strLen = strlen($zfc);
                                    if ($strLen <= $max_len * 3) {
                                        $max_len = ($strLen / 3);
                                    }
                                    // 还剩最后一个字
                                    if ($strLen == 3) {

                                        $fcContainerUtf8[] = $zfc;
                                        break;
                                    }
                                    // 初始截取位置
                                    $g = $strLen - 3 * $max_len - 3;

                                    $tempCount = 0;
                                    $isExitTerm = false;
                                    // 重新循环比对，跳过下面的代码
                                    continue;
                                }
                            }
                        }
                    }
                }




                // 长词再一次分词
                if (strlen($str_cut) > 6) {

                    // 长词再一次分词
                    // $tempRe = $this->getAllTerm($str_cut, true);
                    $tempRe = $this->deepFc($str_cut);

                    // $fcContainerUcs = array_merge($fcContainerUcs, $tempRe[0]);
                    $tempRe[1][] = $str_cut;
                    // $fcContainerUtf8 = array_merge($fcContainerUtf8, $tempRe[1]);
                    foreach ($tempRe[1] as $w) {
                        $fcContainerUtf8[] = $w;
                    }
                }
                // 只保存 两个字 的字符 如果 判别1 成功，则不会运行到这里，不成功，则会运行到这里
                else {

                    $fcContainerUtf8[] = $str_cut;
                }


                // 重新赋值字符串
                $zfc = substr($zfc, 0, $g);
                $strLen = strlen($zfc);
                if ($strLen <= $max_len * 3) {
                    $max_len = ($strLen / 3);
                }
                // 初始截取位置
                $g = $strLen - 3 * $max_len - 3;

                $tempCount = 0;
                $isExitTerm = false;
            }

            // 截取到了最后两个字，并且没有比对出任何词语，则将最后一个字，切出来，作为一个词保存
            if (($tempCount == ($max_len - 1)) && !$isExitTerm) {

                $strLen = strlen($zfc);
                // 剩最后一个字符
                if ($strLen == 3) {
                    if (!$type) {

                        $fcContainerUtf8[] = $zfc;
                    }
                    // $str_cut_last_iconv = iconv('utf-8', 'ucs-2be', $zfc);
                    // $fcContainerUcs[] = $str_cut_last_iconv;
                    break;
                } else {
                    // 如果不是进一步分词，则收集单字
                    if (!$type) {
                        $str_cut_last = substr($zfc, -3);

                        $fcContainerUtf8[] = $str_cut_last;
                    }

                    // $str_cut_last_iconv = iconv('utf-8', 'ucs-2be', $str_cut_last);
                    // $fcContainerUcs[] = $str_cut_last_iconv;

                    // 重新赋值字符串
                    $zfc = substr($zfc, 0, -3);
                    $strLen = strlen($zfc);
                    // 剩余字符个数少于等于7个时，字符个数就为最大切分长度
                    if ($strLen <= $max_len * 3) {
                        $max_len = ($strLen / 3);
                    }
                    // 初始截取位置
                    $g = $strLen - 3 * $max_len - 3;
                }

                // 最后剩一个字，无法循环处理，单独放入结果集保存
                if ($strLen == 3) {
                    $fcContainerUtf8[] = $zfc;
                    break;
                }

                $tempCount = 0;
                $isExitTerm = false;
            }
        }

        $fcContainerUtf8 = array_reverse($fcContainerUtf8);
        return $fcContainerUtf8;
    }

    /**
     * 精确分词后，长词再进一步分词
     */
    public function deepFc($zfc)
    {


        //$max_len = $this->dicWordMax/2;
        // 最大比对长度
        $max_len = 6;
        // 转为utf8编码
        // $zfc = iconv('ucs-2be', 'utf-8', $zfc);
        // 原始字符串 utf8
        $zfcOrg = $zfc;

        //return false;
        $fcContainerUcs = [];
        $fcContainerUtf8 = [];

        $strLen = strlen($zfc);

        if ($strLen < $max_len * 3) {
            $max_len = ($strLen / 3) - 1;
        }
        $count = 1; //初始第一圈
        $currCutNum = $max_len; //截取个数 7个字
        $currCutLen = 3 * $max_len; //每次截取的实际长度
        // 初始截取位置
        $currCutPos = $strLen - $currCutLen;

        for ($g = $currCutPos; $g > -1; $g -= 3) {

            // 如果截取长度减到1，则停止所有比对
            if ($currCutNum == 1) {
                break;
            }

            $str_cut = substr($zfc, $g, $currCutLen);

            if (!$this->isDicMap) {
                // $str_cut_iconv = iconv('utf-8', 'ucs-2be', $str_cut);
                $is_wd = $this->IsWord($str_cut);
            } else {
                $is_wd = isset($this->dicMap[$str_cut]);
            }





            // 是一个词语
            if ($is_wd) {
                // ucs-2be 分词保存
                // $fcContainerUcs[] = $str_cut_iconv;
                // utf8 分词保存
                $fcContainerUtf8[] = $str_cut;
            }


            if ($g < 1) { //截取完成一圈
                ++$count; //圈数加1
                --$currCutNum; //截取字数减1

                $currCutLen = 3 * $currCutNum; //每次截取长度
                // 因为会自动-3，所以此处要提前加3
                $g = $strLen - $currCutLen + 3;
            }
        }

        //数组上下翻转，调整顺序 长词在前面
        $fcContainerUtf8 = array_reverse($fcContainerUtf8);

        return [$fcContainerUcs, $fcContainerUtf8];
    }




    /**
     * 精确分词
     */
    public function standard($str, $idf = false)
    {


        $this->resultArr = [];
        $regx = '/([\x{4E00}-\x{9FA5}]+)|([\x{3040}-\x{309F}]+)|([\x{30A0}-\x{30FF}]+)|([\x{AC00}-\x{D7AF}]+)|([a-zA-Z0-9\.]+)|([\-\_\+\!\@\#\$\%\^\&\*\(\)\|\}\{\“\\”：\"\:\?\>\<\,\.\/\'\;\[\]\~\～\！\@\#\￥\%\…\&\*\（\）\—\+\|\}\{\？\》\《\·\。\，\℃\、\.\~\～\；])/u'; //中日韩 数字字母 标点符号
        $regx_zh = '(^[\x{4e00}-\x{9fa5}]+$)'; //中文
        // $str = '互联网上Creating洪水里面有很多said信息包围着要求精品阳光照耀在身上인사말 안녕하세요昨日までの私は、もうどこにもいない。 རང་སྐྱོང་ལྗོངས་བོད་སྐད་ཡིག་ལས་དོན་ཨུ་ལྷན་གཞུང་དོན་ཁང';
        if (preg_match_all($regx, $str,  $mat)) {
            $all = $mat[0]; //全部 没有空内容
            $zh = $mat[1]; //中文
            $diff = array_diff($all, $zh); //除去中文的所有结果 不会有空内容
            $notZh = implode('', $diff); //非中文的所有内容

            foreach ($all as $blk) {
                if (mb_strlen($blk, 'UTF-8') == 0) {
                    continue;
                }

                //允许进行分词的内容 中文
                if (preg_match('/' . $regx_zh . '/u', $blk)) {
                    // if (isset($cache[$blk])) {
                    //     $words = $cache['getTerm_' . $blk];
                    // } else {
                    //     $words = $this->getTerm($blk);
                    //     $cache['getTerm_' . $blk] = $words;
                    // }
                    $words = $this->getTerm($blk);
                    if (is_array($words)) {
                        $this->resultArr = array_merge($this->resultArr, $words);
                    }
                } //不允许分词的内容	数字 字母
                else if (preg_match('/[a-zA-Z0-9]+/u', $blk)) {

                    $this->resultArr[] = $blk;
                }
                //其它内容 日文 韩文 符号 ...
                else {
                    // 单个切分保存到数组
                    for ($w = 0; $w < mb_strlen($blk, 'utf-8'); ++$w) {
                        $this->resultArr[] = mb_substr($blk, $w, 1, 'utf-8');
                    }
                }
            }
        }

        // 提取idf信息
        $idfArr = [];
        if ($idf) {
            $dicInfo = array_values($this->mainDicInfos);
            $dicInfo = array_merge(...$dicInfo);

            foreach ($this->resultArr as $t) {
                if (isset($dicInfo[$t][1])) {
                    $idfArr[$t] = $dicInfo[$t][1];
                }
                // 分词来自 自定义词典或基础词典 不存在idf，使用固定值
                else {
                    $idfArr[$t] = 5;
                }
            }
            return [
                'terms' => $this->resultArr,
                'idf' => $idfArr,
            ];
        } else {
            return $this->resultArr;
        }
    }


    /**
     * ngram分词
     */
    public function segmentNgram($str, $len = 3)
    {
        $this->resultArr = [];
        $regx = '/([\x{4E00}-\x{9FA5}]+)|([\x{3040}-\x{309F}]+)|([\x{30A0}-\x{30FF}]+)|([\x{AC00}-\x{D7AF}]+)|([a-zA-Z0-9\.]+)|([\-\_\+\!\@\#\$\%\^\&\*\(\)\|\}\{\“\\”：\"\:\?\>\<\,\.\/\'\;\[\]\~\～\！\@\#\￥\%\…\&\*\（\）\—\+\|\}\{\？\》\《\·\。\，\℃\、\.\~\～\；])/u'; //中日韩 数字字母 标点符号
        $regx_zh = '(^[\x{4e00}-\x{9fa5}]+$)'; //中文
        // $str = '互联网上Creating洪水里面有很多said信息包围着要求精品阳光照耀在身上인사말 안녕하세요昨日までの私は、もうどこにもいない。 རང་སྐྱོང་ལྗོངས་བོད་སྐད་ཡིག་ལས་དོན་ཨུ་ལྷན་གཞུང་དོན་ཁང';
        if (preg_match_all($regx, $str,  $mat)) {
            $all = $mat[0]; //全部 没有空内容
            $zh = $mat[1]; //中文
            $diff = array_diff($all, $zh); //除去中文的所有结果 不会有空内容
            $notZh = implode('', $diff); //非中文的所有内容

            foreach ($all as $blk) {
                if (mb_strlen($blk, 'UTF-8') == 0) {
                    continue;
                }

                //允许进行分词的内容 中文
                if (preg_match('/' . $regx_zh . '/u', $blk)) {
                    // if (isset($cache[$blk])) {
                    //     $words = $cache['getTerm_' . $blk];
                    // } else {
                    //     $words = $this->getTerm($blk);
                    //     $cache['getTerm_' . $blk] = $words;
                    // }
                    // $words = $this->getTerm($blk);
                    // ngram 分词方法
                    $words = $this->nGram($blk, $len);
                    if (is_array($words)) {
                        $this->resultArr = array_merge($this->resultArr, $words);
                    }
                } //不允许分词的内容	数字 字母
                else if (preg_match('/[a-zA-Z0-9]+/u', $blk)) {

                    $this->resultArr[] = $blk;
                }
                //其它内容 日文 韩文 符号 ...
                else {
                    // 单个切分保存到数组
                    for ($w = 0; $w < mb_strlen($blk, 'utf-8'); ++$w) {
                        $this->resultArr[] = mb_substr($blk, $w, 1, 'utf-8');
                    }
                }
            }
        }

        return $this->resultArr;
    }

    private function ngramCore($str, $len = 3)
    {
        // 字符串长度
        $strLen = strlen($str);
        // 截取窗口的字符字节数
        $len = $len * 3;

        if ($strLen <= $len) {
            return [$str];
        }
        $resArr = [];
        $endPos = $strLen - $len + 1;

        for ($i = 0; $i < $endPos; $i += 3) {
            $resArr[] = substr($str, $i, $len);
        }
        return $resArr;
    }

    /**
     * 开始切分 效果非常不太好，非必要不使用
     * @param $str 待切分的字符串
     * @param $len 指定的字符串切分个数
     */
    private function nGram($str, $len = 3)
    {

        if (is_array($len)) {
            $resArr = [];
            for ($i = $len[0]; $i <= $len[1]; ++$i) {
                $resArr[] = $this->ngramCore($str, $i);
            }
            $resArr = (array)array_unique(array_merge(...$resArr));
        } else {
            $resArr = $this->ngramCore($str, (int)$len);
        }

        return $resArr;
    }

    /**
     * 全分词
     */
    public function segmentAll($str)
    {


        // 此处很重要
        $this->resultArr = [];

        $regx = '/([\x{4E00}-\x{9FA5}]+)|([\x{3040}-\x{309F}]+)|([\x{30A0}-\x{30FF}]+)|([\x{AC00}-\x{D7AF}]+)|([a-zA-Z0-9\.]+)|([\-\_\+\!\@\#\$\%\^\&\*\(\)\|\}\{\“\\”：\"\:\?\>\<\,\.\/\'\;\[\]\~\～\！\@\#\￥\%\…\&\*\（\）\—\+\|\}\{\？\》\《\·\。\，\℃\、\.\~\～\；])/u'; //中日韩 数字字母 标点符号
        $regx_zh = '(^[\x{4e00}-\x{9fa5}]+$)'; //中文
        // $str = '互联网上Creating洪水里面有很多said信息包围着要求精品阳光照耀在身上인사말 안녕하세요昨日までの私は、もうどこにもいない。 རང་སྐྱོང་ལྗོངས་བོད་སྐད་ཡིག་ལས་དོན་ཨུ་ལྷན་གཞུང་དོན་ཁང';
        if (preg_match_all($regx, $str,  $mat)) {
            $all = $mat[0]; //全部 没有空内容
            // $zh = $mat[1]; //中文
            // $diff = array_diff($all, $zh); //除去中文的所有结果 不会有空内容
            // $notZh = implode('', $diff); //非中文的所有内容

            foreach ($all as $blk) {
                if (mb_strlen($blk, 'UTF-8') == 0) {
                    continue;
                }

                //允许进行分词的内容 中文
                if (preg_match('/' . $regx_zh . '/u', $blk)) {
                    // $words = $this->getTerm($blk);
                    // if (isset($cache[$blk])) {
                    //     $words = $cache['getAllTerm_' . $blk];
                    // } else {
                    //     $words = $this->getAllTerm($blk);
                    //     $cache['getAllTerm_' . $blk] = $words;
                    // }

                    $words = $this->getAllTerm($blk);

                    if (is_array($words)) {
                        $this->resultArr = array_merge($this->resultArr, $words);
                    }
                } //不允许分词的内容	数字 字母
                else if (preg_match('/[a-zA-Z0-9]+/u', $blk)) {

                    $this->resultArr[] = $blk;
                }
                //其它内容 日文 韩文 符号 ...
                else {
                    // 单个切分保存到数组
                    for ($w = 0; $w < mb_strlen($blk, 'utf-8'); ++$w) {
                        $this->resultArr[] = mb_substr($blk, $w, 1, 'utf-8');
                    }
                }
            }



            return $this->resultArr;
        }
    }
}
