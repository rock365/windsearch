<?php


// +----------------------------------------------------------------------
// | WindSearch PHP中小站点的默认搜索选择
// +----------------------------------------------------------------------
// | Copyright (c) All rights reserved.
// +----------------------------------------------------------------------
// | Author: https://github.com/rock365
// +----------------------------------------------------------------------
// | Version: 1.0
// +----------------------------------------------------------------------



namespace WindSearch\Index;

use WindSearch\Core\Func;
use WindSearch\Core\Analyzer;
use WindSearch\Core\Geohash;
use WindSearch\Core\Boot;
use WindSearch\Core\BuildTrie;
use WindSearch\Core\Timsort;
use WindSearch\Core\Cache;
use WindSearch\DAO\PDO_sqlite;
use WindSearch\Exceptions\WindException;

ini_set("max_execution_time", 0);
ini_set('memory_limit', '4000M');
class Wind extends Func
{
    private $indexDir = '';
    private $IndexName = '';
    private $segword = [];
    private $geoRadianContair = [];
    private $countId = 0;
    private $onlyIntersectCache = [];
    private $btimapIntersectCache = [];
    private $IntersectCache = [];
    private $hit3Term = false;
    private $hit5Term = false;
    private $hit7Term = false;
    private $hit9Term = false;
    private $hit11Term = false;
    private $hit13Term = false;
    private $hit15Term = false;
    private $hit17Term = false;
    private $height_freq_word = [];
    private $arr_symbol_filter = [];
    private $arr_stop_word_filter = [];
    private $mapping = [];
    private $primarykey = false;
    private $sys_primarykey = false;
    private $primarykeyType = 'Int_Incremental';
    private $isCache = false;
    private $dataTemp = [];
    private $isSynonym = false;
    private $subLen = 135;
    private $intervalNum = 500;
    public function __construct($IndexName = 'default')
    {
        (new Boot)->init();
        $this->checkEnv();
        $this->IndexName = $IndexName;
        $this->indexDir = dirname(__FILE__) . '/windIndex/';
        if (!is_dir($this->indexDir)) {
            mkdir($this->indexDir, 0777);
        }
        if (is_file($this->indexDir . $this->IndexName . '/Mapping')) {
            $this->mapping = json_decode(file_get_contents($this->indexDir . $this->IndexName . '/Mapping'), true);
        }
        $this->primarykey = $this->getPrimarykey();
        $this->sys_primarykey = $this->getSysPrimarykey();
        $this->primarykeyType = $this->getPrimarykeyType();
        parent::__construct($this->primarykeyType);
    }
    private function checkEnv()
    {
        if (version_compare(PHP_VERSION, '7.3.0', '<')) {
            $this->throwWindException('PHP版本必须 ≥ 7.3.0 !', 0);
        }
        if (!is_readable(dirname(__FILE__))) {
            $this->throwWindException('中间件对所处的文件夹，没有读取权限', 0);
        }
        if (!is_writable(dirname(__FILE__))) {
            $this->throwWindException('中间件对所处的文件夹，没有写入权限', 0);
        }
        if (!extension_loaded('pdo_sqlite')) {
            $this->throwWindException('PHP的pdo_sqlite扩展未开启', 0);
        }
        if (!extension_loaded('mbstring')) {
            $this->throwWindException('PHP的mbstring扩展未开启', 0);
        }
    }

    public function config($cfg = [])
    {
        $this->updateConfig($cfg);
    }
    private $searchResMinNum = 100000;
    private $autoCompleteMaxNum = 30;
    private $filterFunHhreshold = 2000;
    private $autoCompletePrefixLen = 5;
    private $indexDataWriteBufferSize = 10000;
    private $getOriginalSourceSize = 50000;
    private function updateConfig($cfg)
    {
        $cfgList = [
            'searchResMinNum',
            'autoCompleteMaxNum',
            'filterFunHhreshold',
            'autoCompletePrefixLen',
            'filterFunHhreshold',
            'autoCompletePrefixLen',
            'indexDataWriteBufferSize',
            'getOriginalSourceSize'
        ];
        if (!empty($cfg)) {
            foreach ($cfg as $k => $v) {
                if (in_array($k, $cfgList)) {
                    $this->$k = (int)$v;
                }
            }
        }
    }

    private function getError($errorNumber, $param = '')
    {
        $mapping = [
            0 =>  $this->IndexName . '下的mapping文件不存在',
            1 => '必须包含主键字段',
            2 => '主键数据不合法',
            3 => '导入数据的字段与mapping配置的字段不完全匹配，此条数据导入失败',
            4 => '存在不合法数据类型',
            5 => $param . '不是有效日期',
        ];
        return [
            'error' => 1,
            'msg' => $mapping[$errorNumber],
        ];
    }

    private function throwWindException($msg, $code)
    {
        throw new WindException($msg, $code);
    }
    public function getMapping()
    {
        return $this->mapping;
    }

    public function getCurrDir()
    {
        return dirname(__FILE__) . '/';
    }

    public function getStorageDir()
    {
        return $this->indexDir . $this->IndexName . '/';
    }

    public function checkIndex()
    {
        if (!is_dir($this->indexDir . $this->IndexName . '/')) {
            return false;
        } else {
            return true;
        }
    }

    public function createIndex($mapping)
    {

        if (!is_dir($this->indexDir . $this->IndexName)) {
            mkdir($this->indexDir . $this->IndexName, 0777);
        }
        if (!is_dir($this->indexDir . $this->IndexName . '/index/')) {
            mkdir($this->indexDir . $this->IndexName . '/index/', 0777);
        }
        $realTimeIndex  = $this->indexDir . $this->IndexName . '/index/real_time_index/';
        if (!is_dir($realTimeIndex)) {
            mkdir($realTimeIndex, 0777);
        }
        $this->createMapping($mapping);
    }

    public function createDir()
    {
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
        if (!is_dir($indexSegDir)) {
            mkdir($indexSegDir, 0777);
        }
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        if (!is_dir($incrementIndexSegDir)) {
            mkdir($incrementIndexSegDir, 0777);
        }
        $summarizedData = $this->indexDir . $this->IndexName . '/summarizedData/';
        if (!is_dir($summarizedData)) {
            mkdir($summarizedData, 0777);
        }
        $allIndex  = $this->indexDir . $this->IndexName . '/index/';
        if (!is_dir($allIndex)) {
            mkdir($allIndex, 0777);
        }
        $realTimeIndex  = $this->indexDir . $this->IndexName . '/index/real_time_index/';
        if (!is_dir($realTimeIndex)) {
            mkdir($realTimeIndex);
        }
        $deleteDir  = $this->indexDir . $this->IndexName . '/index/delete/';
        if (!is_dir($deleteDir)) {
            mkdir($deleteDir, 0777);
        }
        $intervalMappingDir  = $this->indexDir . $this->IndexName . '/index/interval_mapping/';
        if (!is_dir($intervalMappingDir)) {
            mkdir($intervalMappingDir, 0777);
        }
        $autoCompletionIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIndexSeg/';
        if (!is_dir($autoCompletionIndexSegDir)) {
            mkdir($autoCompletionIndexSegDir, 0777);
        }
        $autoCompletionIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/';
        if (!is_dir($autoCompletionIndexSegDir)) {
            mkdir($autoCompletionIndexSegDir, 0777);
        }
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            if (!is_dir($indexSegDir . $field . '/')) {
                mkdir($indexSegDir . $field . '/', 0777);
            }
            if (!is_dir($indexSegDir . $field . '/terms/')) {
                mkdir($indexSegDir . $field . '/terms/', 0777);
            }
            if (!is_dir($incrementIndexSegDir . $field . '/')) {
                mkdir($incrementIndexSegDir . $field . '/', 0777);
            }
            if (!is_dir($incrementIndexSegDir . $field . '/terms/')) {
                mkdir($incrementIndexSegDir . $field . '/terms/', 0777);
            }

            $realTimeIndex = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block/';
            if (!is_dir($realTimeIndex)) {
                mkdir($realTimeIndex, 0777, true);
            }
        }


        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        foreach ($this->mapping['properties']['allFieldType']['numeric'] as $fd) {
            mkdir($indexSegDir . $fd . '/interval/', 0777, true);
            mkdir($incrementIndexSegDir . $fd . '/interval/', 0777, true);
        }
        foreach ($this->mapping['properties']['allFieldType']['date'] as $fd) {
            mkdir($indexSegDir . $fd . '/interval/', 0777, true);
            mkdir($incrementIndexSegDir . $fd . '/interval/', 0777, true);
        }
    }

    public function createMapping($mapping)
    {
        $definedFieldType = ['text', 'keyword', 'numeric', 'date', 'geo_point', 'primarykey'];
        $field = $mapping['field'];
        $availableField = [];
        $availableFieldType = [];
        foreach ($definedFieldType as $t) {
            $availableFieldType[$t] = [];
        }
        $fieldTypeMapping = [];
        $allFieldType = [];
        foreach ($definedFieldType as $t) {
            $allFieldType[$t] = [];
        }
        $sys_id = 'wind_sys_id';
        $notAllowedFieldName = [$sys_id, '_score'];
        $primarykey = false;
        $primarykey_type = false;
        $autoCompletionField = [];
        $field_analyzer_type = [];
        $auto_completion_field = [];
        $pattern = '/^[a-zA-Z][a-zA-Z0-9_]{1,15}$/';
        foreach ($field as $k => $v) {
            if (!isset($v['name'])) {
                continue;
            }
            $fieldName = strtolower($v['name']);
            if (!preg_match($pattern, $fieldName)) {
                $this->throwWindException('字段名称只能以字母开头，且只能包含数字、字母、下划线，大小写不敏感。', 0);
            }
            if (in_array($fieldName, $notAllowedFieldName)) {
                $this->throwWindException('字段名 ' . $fieldName . ' 属于特殊名称，不可用，请更换一个吧', 0);
            }
            $type = isset($v['type']) ? $v['type'] : false;
            $isIndex = (isset($v['index']) && $v['index']) ? true :  false;
            $analyzer = isset($v['analyzer']) ? $v['analyzer'] : '';
            $function =  isset($v['function']) ? $v['function'] : [];
            $auto_completion = isset($function['completion']) ? true : false;
            if ($isIndex) {
                if ($type === false) {
                    $this->throwWindException($fieldName . '字段未设置数据类型', 0);
                } else {
                    if (!in_array($type, $definedFieldType)) {
                        $this->throwWindException('存在不合法数据类型:' . $type, 0);
                    }
                }
            }
            if ($auto_completion) {
                $auto_completion_field[] = $fieldName;
            }
            if ($type) {
                $allFieldType[$type][] = $fieldName;
                $fieldTypeMapping[$fieldName] = $type;
                $field_analyzer_type[$fieldName] = $analyzer;
            }
            $allowCreateIndexFields = ['text', 'keyword', 'geo_point'];
            if (($isIndex && in_array($type, $allowCreateIndexFields)) || ($type === 'primarykey')) {
                if ($type == 'primarykey') {
                    if (!$primarykey) {
                        $primarykey = $fieldName;
                        $primarykey_type = isset($v['primarykey_type']) ? $v['primarykey_type'] : 'Int_Incremental';
                    }
                } else {
                    if ($type) {
                        $availableFieldType[$type][] = $fieldName;
                    } else {
                        $this->throwWindException('被索引的业务字段必须设置type', 0);
                    }
                    $func = isset($v['function']) ? $v['function'] : false;
                    if ($func) {
                        if (isset($func['completion'])) {
                            $autoCompletionField[$fieldName] = true;
                        }
                    }
                    $availableField[] = $v;
                }
            }
        }
        if (!$primarykey) {
            $this->throwWindException('必须配置主键字段（type => primarykey）', 0);
        }
        $mapping['all_field'] = $field;
        $mapping['field'] = $availableField;
        $mapping['availableFieldType'] = $availableFieldType;
        $mapping['allFieldType'] = $allFieldType;
        $mapping['primarykey'] = $primarykey;
        $mapping['sys_primarykey'] = $sys_id;
        $mapping['primarykey_type'] = $primarykey_type;
        $mapping['auto_completion'] = $autoCompletionField;
        $mapping['fieldtype_mapping'] = $fieldTypeMapping;
        $mapping['field_analyzer_type'] = $field_analyzer_type;
        $mapping['auto_completion_field'] = $auto_completion_field;

        $params = [];
        $params['indexSegNum'] = isset($mapping['param']['indexSegNum']) ? (int)$mapping['param']['indexSegNum'] : 5;
        $params['indexSegNum'] = ($params['indexSegNum'] < 1) ? 5 : $params['indexSegNum'];
        $params['indexSegDataNum'] = isset($mapping['param']['indexSegDataNum']) ? (int)$mapping['param']['indexSegDataNum'] : 10000000;
        $params['indexSegDataNum'] = ($params['indexSegDataNum'] < 100000) ? 100000 : $params['indexSegDataNum'];
        $mapping['param'] = $params;
        $mapping = [
            'index' => $this->IndexName,
            'properties' => $mapping,
        ];
        if (is_file($this->indexDir . $this->IndexName . '/Mapping')) {
            return $this->getError(0);
        }
        file_put_contents($this->indexDir . $this->IndexName . '/Mapping', json_encode($mapping));
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
        if (!is_dir($indexSegDir)) {
            mkdir($indexSegDir, 0777);
        }
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        if (!is_dir($incrementIndexSegDir)) {
            mkdir($incrementIndexSegDir, 0777);
        }
        $autoCompletionIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIndexSeg/';
        if (!is_dir($autoCompletionIndexSegDir)) {
            mkdir($autoCompletionIndexSegDir, 0777);
        }
        $autoCompletionIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/';
        if (!is_dir($autoCompletionIndexSegDir)) {
            mkdir($autoCompletionIndexSegDir, 0777);
        }
        $allIndex  = $this->indexDir . $this->IndexName . '/index/';
        if (!is_dir($allIndex)) {
            mkdir($allIndex, 0777);
        }
        $summarizedData = $this->indexDir . $this->IndexName . '/summarizedData/';
        if (!is_dir($summarizedData)) {
            mkdir($summarizedData, 0777);
        }
        $realTimeIndex  = $this->indexDir . $this->IndexName . '/index/real_time_index/';
        if (!is_dir($realTimeIndex)) {
            mkdir($realTimeIndex, 0777);
        }
        $deleteDir  = $this->indexDir . $this->IndexName . '/index/delete/';
        if (!is_dir($deleteDir)) {
            mkdir($deleteDir, 0777);
        }
        $intervalMappingDir  = $this->indexDir . $this->IndexName . '/index/interval_mapping/';
        if (!is_dir($intervalMappingDir)) {
            mkdir($intervalMappingDir, 0777);
        }
        if (is_file($this->indexDir . $this->IndexName . '/Mapping')) {
            $this->mapping = json_decode(file_get_contents($this->indexDir . $this->IndexName . '/Mapping'), true);
        }
        $this->primarykey = $this->getPrimarykey();
        $this->sys_primarykey = $this->getSysPrimarykey();
        $this->primarykeyType = $this->getPrimarykeyType();
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            if (!is_dir($indexSegDir . $field . '/')) {
                mkdir($indexSegDir . $field . '/', 0777);
            }
            if (!is_dir($indexSegDir . $field . '/terms/')) {
                mkdir($indexSegDir . $field . '/terms/', 0777);
            }
            if (!is_dir($incrementIndexSegDir . $field . '/')) {
                mkdir($incrementIndexSegDir . $field . '/', 0777);
            }
            if (!is_dir($incrementIndexSegDir . $field . '/terms/')) {
                mkdir($incrementIndexSegDir . $field . '/terms/', 0777);
            }
            $realTimeIndex = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/';
            if (!is_dir($realTimeIndex)) {
                mkdir($realTimeIndex, 0777);
            }
            $realTimeIndex = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block/';
            if (!is_dir($realTimeIndex)) {
                mkdir($realTimeIndex, 0777);
            }
        }


        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        foreach ($this->mapping['properties']['allFieldType']['numeric'] as $fd) {
            mkdir($indexSegDir . $fd . '/interval/', 0777, true);
            mkdir($incrementIndexSegDir . $fd . '/interval/', 0777, true);
        }
        foreach ($this->mapping['properties']['allFieldType']['date'] as $fd) {
            mkdir($indexSegDir . $fd . '/interval/', 0777, true);
            mkdir($incrementIndexSegDir . $fd . '/interval/', 0777, true);
        }
        $this->initSqlite();
    }

    private function getPrimarykey()
    {
        $primarykey = isset($this->mapping['properties']['primarykey']) ? $this->mapping['properties']['primarykey'] : false;
        return $primarykey;
    }

    private function getSysPrimarykey()
    {
        $sys_primarykey = isset($this->mapping['properties']['sys_primarykey']) ? $this->mapping['properties']['sys_primarykey'] : false;
        return $sys_primarykey;
    }

    private function getPrimarykeyType()
    {
        $primarykey_type = isset($this->mapping['properties']['primarykey_type']) ? $this->mapping['properties']['primarykey_type'] : 'Int_Incremental';
        return $primarykey_type;
    }

    public function onSynonym()
    {
        $this->isSynonym = true;
    }

    public function closeSynonym()
    {
        $this->isSynonym = false;
    }
    private function loadHeightFreqWord()
    {
        $this->height_freq_word = json_decode(file_get_contents(dirname(__FILE__) . '/windIndexCore/height_freq_word/height_freq_word_json.txt'), true);
    }
    private function loadSymbolStopword()
    {
        if (empty($this->arr_symbol_filter)) {
            $arr_symbol_filter = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/symbol/symbol_txt.txt'));
            $this->arr_symbol_filter = $arr_symbol_filter;
        }
        if (empty($this->arr_stop_word_filter)) {
            $arr_stop_word_filter = (array)explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/stopword/stopword_big.txt'));
            $this->arr_stop_word_filter = $arr_stop_word_filter;
        }
    }
    private function filterSymbolStopWord($fc_arr)
    {
        $fc_arr = array_diff($fc_arr, $this->arr_symbol_filter);
        $fc_arr = array_diff($fc_arr, $this->arr_stop_word_filter);
        return $fc_arr;
    }

    public function buildSensitiveIndex($arr)
    {
        $obj = new BuildTrie('sensitive');
        foreach ($arr as $word) {
            $obj->insert($word);
        }
        return $obj->storage();
    }

    public function checkSensitive($word)
    {
        $obj = new BuildTrie('sensitive');
        return $obj->checkSensitive($word);
    }
    public function checkSensitiveReplace($word, $replace = '**')
    {
        $obj = new BuildTrie('sensitive');
        return $obj->checkSensitiveReplace($word, $replace);
    }
    public function getAllSensitiveWords($word)
    {
        $obj = new BuildTrie('sensitive');
        return $obj->getAllSensitiveWords($word);
    }
    public function intersection($arrays = [])
    {
        $arrays = func_get_args();
        return $this->multi_intersection(...$arrays);
    }

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
        rmdir($dir);
    }

    private static function empty_dir($dir)
    {
        if (substr($dir, -1) != '/') {
            $dir = $dir . '/';
        }
        if (!is_dir($dir)) {
            mkdir($dir, 0777);
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
        rmdir($dir);
    }

    public function checkDateString($date)
    {
        return $this->isValidDateString($date);
    }
    private $fcHandle;

    public function loadCustomAnalyzer($currAnalyzer)
    {
        if (!is_file(dirname(__FILE__) . '/plugin/' . $currAnalyzer . '/' . $currAnalyzer . '.php')) {
            $this->throwWindException('plugin/' . $currAnalyzer . '/' . $currAnalyzer . '.php 文件不存在', 0);
        }
        require_once 'plugin/' . $currAnalyzer . '/' . $currAnalyzer . '.php';
        $class = '\WindSearch\Plugin\\' . $currAnalyzer;
        $this->fcHandle = new $class();
    }

    public function loadAnalyzer($isMap = false)
    {
        $this->fcHandle = new Analyzer();
        if ($isMap) {
            $this->fcHandle->loadDicMap();
        }
    }

    public function buildSynonymIndex()
    {
        $this->loadAnalyzer();
        $synonymDir = dirname(__FILE__) . '/windIndexCore/synonym/';
        $semiFinishedDir = dirname(__FILE__) . '/windIndexCore/synonym/semiFinished/';
        self::del_dir($semiFinishedDir);
        sleep(1);
        if (!is_dir($semiFinishedDir)) {
            mkdir($semiFinishedDir);
        }
        $zm = array(
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'
        );
        $zm2 = array(
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'
        );
        foreach ($zm as $v) {
            foreach ($zm2 as $c) {
                if (!is_dir($semiFinishedDir . $v . '/')) {
                    mkdir($semiFinishedDir . $v . '/');
                }
                if (!is_dir($semiFinishedDir . $v . '/' . $c . '/')) {
                    mkdir($semiFinishedDir . $v . '/' . $c . '/');
                }
            }
        }
        $a = file_get_contents($synonymDir . 'source/synonym.txt');
        $a2 = file_get_contents($synonymDir . 'source/synonymCustom.txt');
        $a = $a . PHP_EOL . $a2;
        $arr = explode(PHP_EOL, $a);
        $arr2 = [];
        foreach ($arr as $v) {
            $zfc = '';
            if (($v == '') || (substr($v, 0, 1) === '/')) {
                continue;
            }
            $v = strtolower($v);
            if (stristr($v, '-')) {
                $c_arr = explode('-', $v);
                $len = count($c_arr);
                for ($i = 0; $i < $len; ++$i) {
                    $arr2 = [];
                    foreach ($c_arr as $f) {
                        if ($f == $c_arr[$i]) {
                            continue;
                        }
                        if (!isset($arr2[$c_arr[$i]])) {
                            if (stristr($f, '/') == true) {
                                $f = str_replace('/', ',', $f);
                            }
                            $arr2[$c_arr[$i]] = $f;
                        } else {
                            if (stristr($f, '/') == true) {
                                $f = str_replace('/', ',', $f);
                            }
                            $arr2[$c_arr[$i]] .= ',' . $f;
                        }
                    }
                    $szm = $this->getFirstLetter($c_arr[$i]);
                    $szm2 = $this->getFirstLetter(substr($c_arr[$i], 3, 3) ? substr($c_arr[$i], 3, 3) : 'U');
                    $zfc = $arr2[$c_arr[$i]];
                    file_put_contents($semiFinishedDir . $szm . '/' . $szm2 . '/synonymDp.index', $c_arr[$i] . '/' . $zfc . PHP_EOL, FILE_APPEND);
                }
            } else if (stristr($v, '|') == true) {
                $c_arr = explode('|', $v);
                if (stristr($c_arr[1], '/') == true) {
                    $c_arr[1] = str_replace('/', ',', $c_arr[1]);
                }
                if (!isset($arr2[$c_arr[0]])) {
                    $arr2[$c_arr[0]] = $c_arr[1];
                } else {
                    $arr2[$c_arr[0]] = $arr2[$c_arr[0]] . ',' . $c_arr[1];
                }
                $szm = $this->getFirstLetter($c_arr[0]);
                $szm2 = $this->getFirstLetter(substr($c_arr[0], 3, 3) ? substr($c_arr[0], 3, 3) : 'U');
                $zfc = $arr2[$c_arr[0]];
                file_put_contents($semiFinishedDir . $szm . '/' . $szm2 . '/synonymDp.index', $c_arr[0] . '/' . $zfc . PHP_EOL, FILE_APPEND);
            }
        }
        $arr_ = [];
        foreach ($zm as $v) {
            foreach ($zm2 as $c) {
                $dir = $semiFinishedDir . $v . '/' . $c . '/synonymDp.index';
                if (!is_file($dir)) {
                    continue;
                }
                $re = file_get_contents($dir);
                $re = explode(PHP_EOL, $re);
                foreach ($re as $s) {
                    if (stristr($s, '/')) {
                        $c_arr = explode('/', $s);
                        $f = $c_arr[1];
                        if (stristr($f, ',')) {
                            $f_arr = explode(',', $f);
                            $f_arr = array_filter($f_arr);
                            foreach ($f_arr as $k) {
                                $k_arr = $this->segment($k);
                                $k_str = implode(',', $k_arr);
                                if (!isset($arr_[$c_arr[0]])) {
                                    $arr_[$c_arr[0]] = $k_str;
                                } else {
                                    $arr_[$c_arr[0]] = $arr_[$c_arr[0]] . '|' . $k_str;
                                }
                            }
                        } else {
                            $f_arr = $this->segment($f);
                            $f_str = implode(',', $f_arr);
                            $arr_[$c_arr[0]] = $f_str;
                        }
                    }
                }
            }
        }
        $buildArr = [];
        foreach ($arr_ as $v => $c) {
            $buildArr[$v] = '<' . $c . '>';
        }
        $buildArr = serialize($buildArr);
        file_put_contents($synonymDir . 'synonymDp/synonymDp.index', $buildArr);
        $res = [];
        foreach ($arr_ as $v => $c) {
            $res[] = $v . '/' . $c;
        }
        file_put_contents($synonymDir . 'synonymDp/synonymMap.index', implode(PHP_EOL, $res));
        self::del_dir($semiFinishedDir);
        $field = '_synonym';
        $dir = $synonymDir . 'synonymDp/' . $field . '.db';
        if (is_file($dir)) {
            unlink($dir);
        }
        if (is_file($dir . '-journal')) {
            unlink($dir . '-journal');
        }
        $pdo = new PDO_sqlite($dir);

        $sql_table = "CREATE TABLE IF NOT EXISTS $field (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
        $pdo->exec($sql_table);
        $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field . "_term ON " . $field . "(term);";
        $pdo->exec($sql_index);
        $storageDir = $this->getStorageDir();
        $specArr = ['\\', PHP_EOL];
        $i = 0;
        $pdo->beginTransaction();
        $rows = $this->yield_fread_row();
        foreach ($rows($synonymDir . 'synonymDp/synonymMap.index') as $line) {
            $line = trim($line);
            if ($line !== '') {
                ++$i;
                list($q, $d) = explode('/', $line);
                $d = trim($d);
                if ($q !== '') {
                    if (in_array($q, $specArr)) {
                        continue;
                    }
                    $sql = "insert into $field (term,ids)values('$q','$d')";
                    $pdo->exec($sql);
                    if (($i % 10000) == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
        }
        $pdo->commit();
        return true;
    }

    private function filterSymbol_StopWord($text, $fc_arr)
    {
        $arr_symbol_filter = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/symbol/symbol_txt.txt'));
        $fc_arr = array_diff($fc_arr, $arr_symbol_filter);
        $arr_stop_word_filter = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/stopword/stopword_small.txt'));
        $fc_arr = array_diff($fc_arr, $arr_stop_word_filter);
        return $fc_arr;
    }
    private $long_short_word_mapping = false;

    private function cutLongWords($str)
    {
        if (!$this->long_short_word_mapping) {
            $mappingfile = dirname(__FILE__) . '/windIndexCore/build_word_segment_mapping/mapping';
            $mapping = json_decode(file_get_contents($mappingfile), true);
            $this->long_short_word_mapping = $mapping;
        }
        if (is_array($this->long_short_word_mapping)) {
            return strtr($str, $this->long_short_word_mapping);
        } else {
            return $str;
        }
    }

    private function stringPreprocessing($str)
    {
        $original_number = [];
        if (preg_match_all("/[a-zA-Z0-9\.]+/i", $str, $mat)) {
            $original_number = $mat[0];
        };

        $str = preg_replace('#([a-zA-Z]{4,})#i', ' $1 ', $str);


        $str = $this->cutLongWords($str);
        $str = preg_replace('#([ ]{2,})#i', ' ', $str);




        $str = preg_replace('#(\_|\-|\.|\/|@|\#|\%|\+|\*)#i', ' $1 ', $str);
        if (!empty($original_number)) {
            $str .= ' ' . implode(' ', $original_number);
        }
        return $str;
    }
    private function cn2num($string)
    {
        if (is_numeric($string)) {
            return $string;
        }
        $replace = [
            '仟' => '千',
            '佰' => '百',
            '拾' => '十',
        ];
        $string = strtr($string, $replace);
        $num = 0;
        $yi = explode('亿', $string);
        if (count($yi) > 1) {
            $num += $this->cn2num($yi[0]) * 100000000;
            $string = $yi[1];
        }
        $wan = explode('万', $string);
        if (count($wan) > 1) {
            $num += $this->cn2num($wan[0]) * 10000;
            $string = $wan[1];
        }
        $qian = explode('千', $string);
        if (count($qian) > 1) {
            $num += $this->cn2num($qian[0]) * 1000;
            $string = $qian[1];
        }
        $bai = explode('百', $string);
        if (count($bai) > 1) {
            $num += $this->cn2num($bai[0]) * 100;
            $string = $bai[1];
        }
        $shi = explode('十', $string);
        if (count($shi) > 1) {
            $num += $this->cn2num($shi[0] ? $shi[0] : '一') * 10;
            $string = $shi[1] ? $shi[1] : '零';
        }
        $ling = explode('零', $string);
        if (count($ling) > 1) {
            $string = $ling[1];
        }
        $d = array(
            '一' => '1', '二' => '2', '三' => '3', '四' => '4', '五' => '5', '六' => '6', '七' => '7', '八' => '8', '九' => '9',
            '壹' => '1', '贰' => '2', '叁' => '3', '肆' => '4', '伍' => '5', '陆' => '6', '柒' => '7', '捌' => '8', '玖' => '9',
            '零' => 0, '0' => 0, 'O' => 0, 'o' => 0,
            '两' => 2,
            '1' => 1, '2' => 2, '3' => 3, '4' => 4, '5' => 5, '6' => 6, '7' => 7, '8' => 8, '9' => 9, '0' => 0
        );
        return $num + (isset($d[$string]) ? $d[$string] : 0);
    }

    private function ZhToNumber($str)
    {
        $arr_2 = array(
            '壹' => '一', '贰' => '二', '叁' => '三', '肆' => '四', '伍' => '五', '陆' => '六', '柒' => '七', '捌' => '八', '玖' => '九',
            '两' => '二',
            '仟' => '千', '佰' => '百', '拾' => '十',
            '万万' => '亿'
        );
        $str = strtr($str, $arr_2);
        $map = array(
            '一' => '1', '二' => '2', '三' => '3', '四' => '4', '五' => '5', '六' => '6', '七' => '7', '八' => '8', '九' => '9',
            '壹' => '1', '贰' => '2', '叁' => '3', '肆' => '4', '伍' => '5', '陆' => '6', '柒' => '7', '捌' => '8', '玖' => '9',
            '两' => '2',
            '仟' => '千', '佰' => '百', '拾' => '十',
            '万万' => '亿',
        );
        $str = strtr($str, $map);
        $res = [];
        if (preg_match('/([亿万千百十零]+)/u', $str, $rettemp)) {
            if (preg_match_all('/([\d亿万千百十零]+)/u', $str, $ret)) {
                foreach ($ret[0] as $v) {
                    $res[] = $this->cn2num($v);
                }
            }
        }
        return $res;
    }

    private function traditionalToSimplified($word)
    {
        $arr = array(
            '內' => '内',
            '勻' => '匀',
            '弔' => '吊',
            '戶' => '户',
            '冊' => '册',
            '朮' => '术',
            '氾' => '泛',
            '丟' => '丢',
            '亙' => '亘',
            '伕' => '夫',
            '兇' => '凶',
            '吋' => '寸',
            '汙' => '污',
            '汎' => '泛',
            '佇' => '伫',
            '佔' => '占',
            '佈' => '布',
            '兌' => '兑',
            '別' => '别',
            '刪' => '删',
            '呎' => '尺',
            '吳' => '吴',
            '呂' => '吕',
            '吶' => '呐',
            '囪' => '囱',
            '壯' => '壮',
            '夾' => '夹',
            '妝' => '妆',
            '決' => '决',
            '沖' => '冲',
            '沒' => '没',
            '沍' => '冱',
            '災' => '灾',
            '牠' => '它',
            '禿' => '秃',
            '見' => '见',
            '貝' => '贝',
            '車' => '车',
            '迆' => '迤',
            '釆' => '采',
            '阬' => '坑',
            '並' => '并',
            '亞' => '亚',
            '來' => '来',
            '併' => '并',
            '侖' => '仑',
            '兒' => '儿',
            '兩' => '两',
            '協' => '协',
            '卹' => '恤',
            '姍' => '姗',
            '妳' => '你',
            '屆' => '届',
            '岡' => '冈',
            '彿' => '佛',
            '拋' => '抛',
            '於' => '于',
            '昇' => '升',
            '東' => '东',
            '歿' => '殁',
            '況' => '况',
            '爭' => '争',
            '狀' => '状',
            '玨' => '珏',
            '秈' => '籼',
            '糾' => '纠',
            '羋' => '芈',
            '臥' => '卧',
            '軋' => '轧',
            '長' => '长',
            '門' => '门',
            '俠' => '侠',
            '侶' => '侣',
            '係' => '系',
            '侷' => '局',
            '兗' => '兖',
            '冑' => '胄',
            '剎' => '刹',
            '剋' => '克',
            '則' => '则',
            '勁' => '劲',
            '卻' => '却',
            '奐' => '奂',
            '姪' => '侄',
            '姦' => '奸',
            '屍' => '尸',
            '帥' => '帅',
            '彥' => '彦',
            '後' => '后',
            '恆' => '恒',
            '柵' => '栅',
            '枴' => '拐',
            '洶' => '汹',
            '洩' => '泄',
            '為' => '为',
            '炤' => '照',
            '牴' => '抵',
            '盃' => '杯',
            '紂' => '纣',
            '紅' => '红',
            '紀' => '纪',
            '紉' => '纫',
            '紇' => '纥',
            '約' => '约',
            '紆' => '纡',
            '耑' => '专',
            '苧' => '苎',
            '觔' => '筋',
            '計' => '计',
            '訂' => '订',
            '訃' => '讣',
            '貞' => '贞',
            '負' => '负',
            '軍' => '军',
            '軌' => '轨',
            '郃' => '合',
            '閂' => '闩',
            '韋' => '韦',
            '頁' => '页',
            '風' => '风',
            '飛' => '飞',
            '倣' => '仿',
            '倖' => '幸',
            '倆' => '俩',
            '們' => '们',
            '倀' => '伥',
            '個' => '个',
            '倫' => '伦',
            '倉' => '仓',
            '凍' => '冻',
            '剛' => '刚',
            '剝' => '剥',
            '員' => '员',
            '娛' => '娱',
            '孫' => '孙',
            '宮' => '宫',
            '峽' => '峡',
            '島' => '岛',
            '峴' => '岘',
            '師' => '师',
            '庫' => '库',
            '徑' => '径',
            '恥' => '耻',
            '悅' => '悦',
            '挾' => '挟',
            '時' => '时',
            '晉' => '晋',
            '書' => '书',
            '氣' => '气',
            '涇' => '泾',
            '浬' => '里',
            '浹' => '浃',
            '烏' => '乌',
            '狹' => '狭',
            '狽' => '狈',
            '玆' => '兹',
            '珮' => '佩',
            '珪' => '圭',
            '畝' => '亩',
            '皰' => '疱',
            '砲' => '炮',
            '祕' => '秘',
            '祐' => '佑',
            '紡' => '纺',
            '紗' => '纱',
            '紋' => '纹',
            '純' => '纯',
            '紐' => '纽',
            '紕' => '纰',
            '級' => '级',
            '紜' => '纭',
            '納' => '纳',
            '紙' => '纸',
            '紛' => '纷',
            '脅' => '胁',
            '脈' => '脉',
            '芻' => '刍',
            '荊' => '荆',
            '茲' => '兹',
            '衹' => '只',
            '記' => '记',
            '訐' => '讦',
            '討' => '讨',
            '訌' => '讧',
            '訕' => '讪',
            '訊' => '讯',
            '託' => '托',
            '訓' => '训',
            '訖' => '讫',
            '訏' => '吁',
            '豈' => '岂',
            '財' => '财',
            '貢' => '贡',
            '軒' => '轩',
            '軔' => '轫',
            '迺' => '乃',
            '迴' => '回',
            '釘' => '钉',
            '針' => '针',
            '釗' => '钊',
            '釙' => '钋',
            '閃' => '闪',
            '陣' => '阵',
            '陝' => '陕',
            '陘' => '陉',
            '陞' => '升',
            '隻' => '只',
            '飢' => '饥',
            '馬' => '马',
            '鬥' => '斗',
            '偽' => '伪',
            '偉' => '伟',
            '偵' => '侦',
            '側' => '侧',
            '務' => '务',
            '動' => '动',
            '區' => '区',
            '參' => '参',
            '啞' => '哑',
            '問' => '问',
            '唸' => '念',
            '啣' => '衔',
            '國' => '国',
            '堅' => '坚',
            '堊' => '垩',
            '執' => '执',
            '夠' => '够',
            '婁' => '娄',
            '婦' => '妇',
            '專' => '专',
            '將' => '将',
            '屜' => '屉',
            '崢' => '峥',
            '崑' => '昆',
            '崙' => '仑',
            '崗' => '岗',
            '帶' => '带',
            '帳' => '帐',
            '張' => '张',
            '強' => '强',
            '彫' => '雕',
            '從' => '从',
            '徠' => '徕',
            '悽' => '凄',
            '悵' => '怅',
            '惇' => '敦',
            '捲' => '卷',
            '掃' => '扫',
            '掛' => '挂',
            '捫' => '扪',
            '掄' => '抡',
            '掙' => '挣',
            '採' => '采',
            '捨' => '舍',
            '敗' => '败',
            '啟' => '启',
            '敘' => '叙',
            '斬' => '斩',
            '晝' => '昼',
            '勗' => '勖',
            '桿' => '杆',
            '梱' => '捆',
            '棄' => '弃',
            '梔' => '栀',
            '條' => '条',
            '梟' => '枭',
            '殺' => '杀',
            '毬' => '球',
            '氫' => '氢',
            '涼' => '凉',
            '淺' => '浅',
            '淵' => '渊',
            '淒' => '凄',
            '淚' => '泪',
            '淪' => '沦',
            '淨' => '净',
            '牽' => '牵',
            '猙' => '狰',
            '現' => '现',
            '琍' => '璃',
            '產' => '产',
            '畢' => '毕',
            '異' => '异',
            '眾' => '众',
            '硃' => '朱',
            '絆' => '绊',
            '絃' => '弦',
            '統' => '统',
            '紮' => '扎',
            '紹' => '绍',
            '紼' => '绋',
            '絀' => '绌',
            '細' => '细',
            '紳' => '绅',
            '組' => '组',
            '終' => '终',
            '紲' => '绁',
            '紱' => '绂',
            '缽' => '钵',
            '習' => '习',
            '脣' => '唇',
            '脫' => '脱',
            '脩' => '修',
            '莢' => '荚',
            '莖' => '茎',
            '莊' => '庄',
            '莧' => '苋',
            '處' => '处',
            '術' => '术',
            '袞' => '衮',
            '覓' => '觅',
            '規' => '规',
            '訪' => '访',
            '訝' => '讶',
            '訣' => '诀',
            '訥' => '讷',
            '許' => '许',
            '設' => '设',
            '訟' => '讼',
            '訛' => '讹',
            '訢' => '欣',
            '販' => '贩',
            '責' => '责',
            '貫' => '贯',
            '貨' => '货',
            '貪' => '贪',
            '貧' => '贫',
            '軛' => '轭',
            '軟' => '软',
            '這' => '这',
            '連' => '连',
            '逕' => '迳',
            '釵' => '钗',
            '釦' => '扣',
            '釣' => '钓',
            '釧' => '钏',
            '釩' => '钒',
            '閉' => '闭',
            '陳' => '陈',
            '陸' => '陆',
            '陰' => '阴',
            '頂' => '顶',
            '頃' => '顷',
            '魚' => '鱼',
            '鳥' => '鸟',
            '鹵' => '卤',
            '麥' => '麦',
            '傢' => '家',
            '備' => '备',
            '傑' => '杰',
            '傖' => '伧',
            '傘' => '伞',
            '傚' => '效',
            '凱' => '凯',
            '剴' => '剀',
            '創' => '创',
            '勞' => '劳',
            '勝' => '胜',
            '勛' => '勋',
            '喪' => '丧',
            '單' => '单',
            '喲' => '哟',
            '喚' => '唤',
            '喬' => '乔',
            '喫' => '吃',
            '圍' => '围',
            '堯' => '尧',
            '場' => '场',
            '報' => '报',
            '堝' => '埚',
            '壺' => '壶',
            '媧' => '娲',
            '尋' => '寻',
            '嵐' => '岚',
            '幀' => '帧',
            '幃' => '帏',
            '幾' => '几',
            '廁' => '厕',
            '廂' => '厢',
            '廄' => '厩',
            '復' => '复',
            '惡' => '恶',
            '悶' => '闷',
            '愜' => '惬',
            '惻' => '恻',
            '惱' => '恼',
            '揀' => '拣',
            '揮' => '挥',
            '換' => '换',
            '揚' => '扬',
            '揹' => '背',
            '棗' => '枣',
            '棟' => '栋',
            '棧' => '栈',
            '棲' => '栖',
            '欽' => '钦',
            '殘' => '残',
            '殼' => '壳',
            '氬' => '氩',
            '湧' => '涌',
            '湊' => '凑',
            '減' => '减',
            '渦' => '涡',
            '湯' => '汤',
            '測' => '测',
            '渾' => '浑',
            '渙' => '涣',
            '湣' => '泯',
            '無' => '无',
            '猶' => '犹',
            '琺' => '珐',
            '琯' => '管',
            '甦' => '苏',
            '畫' => '画',
            '痙' => '痉',
            '痠' => '酸',
            '發' => '发',
            '盜' => '盗',
            '睏' => '困',
            '硯' => '砚',
            '稈' => '秆',
            '稅' => '税',
            '筆' => '笔',
            '筍' => '笋',
            '絞' => '绞',
            '結' => '结',
            '絨' => '绒',
            '絕' => '绝',
            '絲' => '丝',
            '絡' => '络',
            '給' => '给',
            '絢' => '绚',
            '絰' => '绖',
            '絳' => '绛',
            '肅' => '肃',
            '腎' => '肾',
            '脹' => '胀',
            '華' => '华',
            '萊' => '莱',
            '萇' => '苌',
            '虛' => '虚',
            '視' => '视',
            '註' => '注',
            '詠' => '咏',
            '評' => '评',
            '詞' => '词',
            '証' => '证',
            '詁' => '诂',
            '詔' => '诏',
            '詛' => '诅',
            '詐' => '诈',
            '詆' => '诋',
            '訴' => '诉',
            '診' => '诊',
            '訶' => '诃',
            '詖' => '诐',
            '貯' => '贮',
            '貼' => '贴',
            '貳' => '贰',
            '貽' => '贻',
            '賁' => '贲',
            '費' => '费',
            '賀' => '贺',
            '貴' => '贵',
            '買' => '买',
            '貶' => '贬',
            '貿' => '贸',
            '貸' => '贷',
            '軻' => '轲',
            '軸' => '轴',
            '軼' => '轶',
            '週' => '周',
            '進' => '进',
            '郵' => '邮',
            '鄉' => '乡',
            '鈔' => '钞',
            '鈕' => '钮',
            '鈣' => '钙',
            '鈉' => '钠',
            '鈞' => '钧',
            '鈍' => '钝',
            '鈐' => '钤',
            '鈑' => '钣',
            '閔' => '闵',
            '閏' => '闰',
            '開' => '开',
            '閑' => '闲',
            '間' => '间',
            '閒' => '闲',
            '閎' => '闳',
            '隊' => '队',
            '階' => '阶',
            '陽' => '阳',
            '隄' => '堤',
            '雲' => '云',
            '韌' => '韧',
            '項' => '项',
            '順' => '顺',
            '須' => '须',
            '飪' => '饪',
            '飯' => '饭',
            '飩' => '饨',
            '飲' => '饮',
            '飭' => '饬',
            '馮' => '冯',
            '馭' => '驭',
            '黃' => '黄',
            '亂' => '乱',
            '傭' => '佣',
            '債' => '债',
            '傳' => '传',
            '僅' => '仅',
            '傾' => '倾',
            '傷' => '伤',
            '傯' => '偬',
            '剷' => '铲',
            '勦' => '剿',
            '勢' => '势',
            '勣' => '绩',
            '匯' => '汇',
            '嗎' => '吗',
            '嗇' => '啬',
            '嗚' => '呜',
            '嗆' => '呛',
            '園' => '园',
            '圓' => '圆',
            '塗' => '涂',
            '塚' => '冢',
            '塭' => '瘟',
            '塊' => '块',
            '塢' => '坞',
            '塒' => '埘',
            '塋' => '茔',
            '奧' => '奥',
            '媽' => '妈',
            '媼' => '媪',
            '幹' => '干',
            '廈' => '厦',
            '弒' => '弑',
            '彙' => '汇',
            '徬' => '旁',
            '愛' => '爱',
            '慄' => '栗',
            '慍' => '愠',
            '愾' => '忾',
            '愴' => '怆',
            '愷' => '恺',
            '搾' => '榨',
            '損' => '损',
            '搶' => '抢',
            '搖' => '摇',
            '搗' => '捣',
            '搆' => '构',
            '暉' => '晖',
            '暈' => '晕',
            '暘' => '旸',
            '會' => '会',
            '業' => '业',
            '極' => '极',
            '楊' => '杨',
            '楨' => '桢',
            '楓' => '枫',
            '歲' => '岁',
            '毀' => '毁',
            '溝' => '沟',
            '滅' => '灭',
            '溼' => '湿',
            '溫' => '温',
            '準' => '准',
            '滄' => '沧',
            '煙' => '烟',
            '煩' => '烦',
            '煉' => '炼',
            '煬' => '炀',
            '煥' => '焕',
            '煖' => '暖',
            '爺' => '爷',
            '獅' => '狮',
            '瑯' => '琅',
            '琿' => '珲',
            '當' => '当',
            '痲' => '麻',
            '痺' => '痹',
            '痳' => '麻',
            '盞' => '盏',
            '睞' => '睐',
            '睪' => '睾',
            '睜' => '睁',
            '祿' => '禄',
            '萬' => '万',
            '稜' => '棱',
            '稟' => '禀',
            '節' => '节',
            '筧' => '笕',
            '粵' => '粤',
            '經' => '经',
            '絹' => '绢',
            '綑' => '困',
            '綁' => '绑',
            '綏' => '绥',
            '絛' => '绦',
            '義' => '义',
            '羨' => '羡',
            '聖' => '圣',
            '腸' => '肠',
            '腳' => '脚',
            '腫' => '肿',
            '腦' => '脑',
            '葷' => '荤',
            '葦' => '苇',
            '葉' => '叶',
            '萵' => '莴',
            '虜' => '虏',
            '號' => '号',
            '蛻' => '蜕',
            '蜆' => '蚬',
            '補' => '补',
            '裝' => '装',
            '裡' => '里',
            '裊' => '袅',
            '覜' => '眺',
            '詫' => '诧',
            '該' => '该',
            '詳' => '详',
            '試' => '试',
            '詩' => '诗',
            '詰' => '诘',
            '誇' => '夸',
            '詼' => '诙',
            '詣' => '诣',
            '誠' => '诚',
            '話' => '话',
            '誅' => '诛',
            '詭' => '诡',
            '詢' => '询',
            '詮' => '诠',
            '詬' => '诟',
            '賊' => '贼',
            '資' => '资',
            '賈' => '贾',
            '賄' => '贿',
            '貲' => '赀',
            '賃' => '赁',
            '賂' => '赂',
            '賅' => '赅',
            '跡' => '迹',
            '較' => '较',
            '載' => '载',
            '軾' => '轼',
            '輊' => '轾',
            '農' => '农',
            '運' => '运',
            '遊' => '游',
            '達' => '达',
            '違' => '违',
            '過' => '过',
            '鄒' => '邹',
            '鈷' => '钴',
            '鉗' => '钳',
            '鈸' => '钹',
            '鈽' => '钸',
            '鉀' => '钾',
            '鈾' => '铀',
            '鉛' => '铅',
            '鉋' => '刨',
            '鉤' => '钩',
            '鉑' => '铂',
            '鈴' => '铃',
            '鉉' => '铉',
            '鉍' => '铋',
            '鉅' => '钜',
            '鈹' => '铍',
            '鈿' => '钿',
            '鉚' => '铆',
            '閘' => '闸',
            '隕' => '陨',
            '雋' => '隽',
            '電' => '电',
            '預' => '预',
            '頑' => '顽',
            '頓' => '顿',
            '頊' => '顼',
            '頒' => '颁',
            '頌' => '颂',
            '飼' => '饲',
            '飴' => '饴',
            '飽' => '饱',
            '飾' => '饰',
            '馳' => '驰',
            '馱' => '驮',
            '馴' => '驯',
            '鳩' => '鸠',
            '僥' => '侥',
            '僕' => '仆',
            '僑' => '侨',
            '僱' => '雇',
            '劃' => '划',
            '匱' => '匮',
            '厭' => '厌',
            '嘗' => '尝',
            '嘔' => '呕',
            '嘆' => '叹',
            '嘍' => '喽',
            '嘖' => '啧',
            '嗶' => '哔',
            '團' => '团',
            '圖' => '图',
            '塵' => '尘',
            '墊' => '垫',
            '塹' => '堑',
            '壽' => '寿',
            '夢' => '梦',
            '奪' => '夺',
            '奩' => '奁',
            '嫗' => '妪',
            '寧' => '宁',
            '實' => '实',
            '寢' => '寝',
            '對' => '对',
            '屢' => '屡',
            '嶄' => '崭',
            '嶇' => '岖',
            '幣' => '币',
            '幗' => '帼',
            '彆' => '别',
            '徹' => '彻',
            '慇' => '殷',
            '態' => '态',
            '慣' => '惯',
            '慟' => '恸',
            '慚' => '惭',
            '慘' => '惨',
            '摟' => '搂',
            '摑' => '掴',
            '摻' => '掺',
            '暢' => '畅',
            '榮' => '荣',
            '槓' => '杠',
            '構' => '构',
            '槍' => '枪',
            '榦' => '干',
            '槃' => '盘',
            '氳' => '氲',
            '滾' => '滚',
            '漬' => '渍',
            '漢' => '汉',
            '滿' => '满',
            '滯' => '滞',
            '漸' => '渐',
            '漲' => '涨',
            '漣' => '涟',
            '滬' => '沪',
            '漁' => '渔',
            '滲' => '渗',
            '滌' => '涤',
            '滷' => '卤',
            '熒' => '荧',
            '爾' => '尔',
            '犖' => '荦',
            '獄' => '狱',
            '瑤' => '瑶',
            '瑣' => '琐',
            '瑪' => '玛',
            '瘧' => '疟',
            '瘍' => '疡',
            '瘋' => '疯',
            '瘉' => '愈',
            '瘓' => '痪',
            '盡' => '尽',
            '監' => '监',
            '碩' => '硕',
            '禎' => '祯',
            '禍' => '祸',
            '種' => '种',
            '稱' => '称',
            '窪' => '洼',
            '窩' => '窝',
            '箋' => '笺',
            '箏' => '筝',
            '箇' => '个',
            '綻' => '绽',
            '綰' => '绾',
            '綜' => '综',
            '綽' => '绰',
            '綾' => '绫',
            '綠' => '绿',
            '緊' => '紧',
            '綴' => '缀',
            '網' => '网',
            '綱' => '纲',
            '綺' => '绮',
            '綢' => '绸',
            '綿' => '绵',
            '綵' => '彩',
            '綸' => '纶',
            '維' => '维',
            '緒' => '绪',
            '緇' => '缁',
            '綬' => '绶',
            '罰' => '罚',
            '聞' => '闻',
            '臺' => '台',
            '與' => '与',
            '蓆' => '席',
            '蒞' => '莅',
            '蓋' => '盖',
            '蓀' => '荪',
            '蒼' => '苍',
            '蝕' => '蚀',
            '製' => '制',
            '誦' => '诵',
            '誌' => '志',
            '語' => '语',
            '誣' => '诬',
            '認' => '认',
            '誡' => '诫',
            '誤' => '误',
            '說' => '说',
            '誥' => '诰',
            '誨' => '诲',
            '誘' => '诱',
            '誑' => '诳',
            '誚' => '诮',
            '貍' => '狸',
            '賓' => '宾',
            '賑' => '赈',
            '賒' => '赊',
            '趙' => '赵',
            '趕' => '赶',
            '跼' => '局',
            '輔' => '辅',
            '輒' => '辄',
            '輕' => '轻',
            '輓' => '挽',
            '遠' => '远',
            '遜' => '逊',
            '遙' => '遥',
            '遞' => '递',
            '鄘' => '墉',
            '鉸' => '铰',
            '銀' => '银',
            '銅' => '铜',
            '銘' => '铭',
            '銖' => '铢',
            '鉻' => '铬',
            '銓' => '铨',
            '銜' => '衔',
            '銨' => '铵',
            '銑' => '铣',
            '閡' => '阂',
            '閨' => '闺',
            '閩' => '闽',
            '閣' => '阁',
            '閥' => '阀',
            '際' => '际',
            '頗' => '颇',
            '領' => '领',
            '颯' => '飒',
            '颱' => '台',
            '餃' => '饺',
            '餅' => '饼',
            '餌' => '饵',
            '餉' => '饷',
            '駁' => '驳',
            '骯' => '肮',
            '鳴' => '鸣',
            '鳶' => '鸢',
            '鳳' => '凤',
            '麼' => '么',
            '齊' => '齐',
            '億' => '亿',
            '儀' => '仪',
            '價' => '价',
            '儂' => '侬',
            '儈' => '侩',
            '儉' => '俭',
            '儅' => '当',
            '凜' => '凛',
            '劇' => '剧',
            '劉' => '刘',
            '劍' => '剑',
            '劊' => '刽',
            '厲' => '厉',
            '嘮' => '唠',
            '嘩' => '哗',
            '噓' => '嘘',
            '噴' => '喷',
            '嘯' => '啸',
            '嘰' => '叽',
            '墳' => '坟',
            '墜' => '坠',
            '墮' => '堕',
            '嫻' => '娴',
            '嬋' => '婵',
            '嫵' => '妩',
            '嬌' => '娇',
            '嬈' => '娆',
            '寬' => '宽',
            '審' => '审',
            '寫' => '写',
            '層' => '层',
            '嶔' => '嵚',
            '幟' => '帜',
            '廢' => '废',
            '廚' => '厨',
            '廟' => '庙',
            '廝' => '厮',
            '廣' => '广',
            '廠' => '厂',
            '彈' => '弹',
            '慶' => '庆',
            '慮' => '虑',
            '憂' => '忧',
            '慼' => '戚',
            '慫' => '怂',
            '慾' => '欲',
            '憐' => '怜',
            '憫' => '悯',
            '憚' => '惮',
            '憤' => '愤',
            '憮' => '怃',
            '摯' => '挚',
            '撲' => '扑',
            '撈' => '捞',
            '撐' => '撑',
            '撥' => '拨',
            '撓' => '挠',
            '撫' => '抚',
            '撚' => '捻',
            '撢' => '掸',
            '撳' => '揿',
            '敵' => '敌',
            '數' => '数',
            '暫' => '暂',
            '暱' => '昵',
            '樣' => '样',
            '槨' => '椁',
            '樁' => '桩',
            '樞' => '枢',
            '標' => '标',
            '樓' => '楼',
            '槳' => '桨',
            '樂' => '乐',
            '樅' => '枞',
            '樑' => '梁',
            '歐' => '欧',
            '歎' => '叹',
            '殤' => '殇',
            '毆' => '殴',
            '漿' => '浆',
            '潑' => '泼',
            '潔' => '洁',
            '澆' => '浇',
            '潛' => '潜',
            '潰' => '溃',
            '潤' => '润',
            '澗' => '涧',
            '潯' => '浔',
            '潟' => '舄',
            '熱' => '热',
            '犛' => '牦',
            '獎' => '奖',
            '瑩' => '莹',
            '瘡' => '疮',
            '皚' => '皑',
            '皺' => '皱',
            '盤' => '盘',
            '瞇' => '眯',
            '確' => '确',
            '碼' => '码',
            '穀' => '谷',
            '窯' => '窑',
            '窮' => '穷',
            '範' => '范',
            '箠' => '棰',
            '締' => '缔',
            '練' => '练',
            '緯' => '纬',
            '緻' => '致',
            '緘' => '缄',
            '緬' => '缅',
            '緝' => '缉',
            '編' => '编',
            '緣' => '缘',
            '線' => '线',
            '緞' => '缎',
            '緩' => '缓',
            '綞' => '缍',
            '緙' => '缂',
            '緲' => '缈',
            '緹' => '缇',
            '罵' => '骂',
            '罷' => '罢',
            '膠' => '胶',
            '膚' => '肤',
            '蓮' => '莲',
            '蔭' => '荫',
            '蔣' => '蒋',
            '蔔' => '卜',
            '蔥' => '葱',
            '蔆' => '菱',
            '蝦' => '虾',
            '蝸' => '蜗',
            '蝨' => '虱',
            '衛' => '卫',
            '衝' => '冲',
            '複' => '复',
            '誼' => '谊',
            '諒' => '谅',
            '談' => '谈',
            '諄' => '谆',
            '誕' => '诞',
            '請' => '请',
            '諸' => '诸',
            '課' => '课',
            '諉' => '诿',
            '諂' => '谄',
            '調' => '调',
            '誰' => '谁',
            '論' => '论',
            '諍' => '诤',
            '誶' => '谇',
            '誹' => '诽',
            '諛' => '谀',
            '豎' => '竖',
            '豬' => '猪',
            '賠' => '赔',
            '賞' => '赏',
            '賦' => '赋',
            '賤' => '贱',
            '賬' => '账',
            '賭' => '赌',
            '賢' => '贤',
            '賣' => '卖',
            '賜' => '赐',
            '質' => '质',
            '賡' => '赓',
            '踐' => '践',
            '踡' => '蜷',
            '輝' => '辉',
            '輛' => '辆',
            '輟' => '辍',
            '輩' => '辈',
            '輦' => '辇',
            '輪' => '轮',
            '輜' => '辎',
            '輞' => '辋',
            '輥' => '辊',
            '適' => '适',
            '遷' => '迁',
            '鄰' => '邻',
            '鄭' => '郑',
            '鄧' => '邓',
            '醃' => '腌',
            '鋅' => '锌',
            '銻' => '锑',
            '銷' => '销',
            '鋪' => '铺',
            '銬' => '铐',
            '鋤' => '锄',
            '鋁' => '铝',
            '銳' => '锐',
            '銼' => '锉',
            '鋒' => '锋',
            '鋇' => '钡',
            '鋰' => '锂',
            '銲' => '焊',
            '閭' => '闾',
            '閱' => '阅',
            '鞏' => '巩',
            '頡' => '颉',
            '頫' => '俯',
            '頜' => '颌',
            '颳' => '刮',
            '養' => '养',
            '餓' => '饿',
            '餒' => '馁',
            '餘' => '馀',
            '駝' => '驼',
            '駐' => '驻',
            '駟' => '驷',
            '駛' => '驶',
            '駑' => '驽',
            '駕' => '驾',
            '駒' => '驹',
            '駙' => '驸',
            '髮' => '发',
            '鬧' => '闹',
            '魷' => '鱿',
            '魯' => '鲁',
            '鴆' => '鸩',
            '鴉' => '鸦',
            '鴃' => '觖',
            '麩' => '麸',
            '齒' => '齿',
            '儘' => '尽',
            '儔' => '俦',
            '儐' => '傧',
            '儕' => '侪',
            '冪' => '幂',
            '劑' => '剂',
            '勳' => '勋',
            '噹' => '当',
            '噸' => '吨',
            '噥' => '哝',
            '噯' => '嗳',
            '墾' => '垦',
            '壇' => '坛',
            '奮' => '奋',
            '嬝' => '袅',
            '學' => '学',
            '導' => '导',
            '彊' => '强',
            '憲' => '宪',
            '憑' => '凭',
            '憊' => '惫',
            '懍' => '懔',
            '憶' => '忆',
            '戰' => '战',
            '擁' => '拥',
            '擋' => '挡',
            '撻' => '挞',
            '據' => '据',
            '擄' => '掳',
            '擇' => '择',
            '撿' => '捡',
            '擔' => '担',
            '撾' => '挝',
            '曆' => '历',
            '曉' => '晓',
            '曄' => '晔',
            '曇' => '昙',
            '樸' => '朴',
            '樺' => '桦',
            '橫' => '横',
            '樹' => '树',
            '橢' => '椭',
            '橋' => '桥',
            '機' => '机',
            '橈' => '桡',
            '歷' => '历',
            '澱' => '淀',
            '濃' => '浓',
            '澤' => '泽',
            '濁' => '浊',
            '澦' => '滪',
            '澠' => '渑',
            '熾' => '炽',
            '燉' => '炖',
            '燐' => '磷',
            '燒' => '烧',
            '燈' => '灯',
            '燙' => '烫',
            '燜' => '焖',
            '燄' => '焰',
            '獨' => '独',
            '璣' => '玑',
            '甌' => '瓯',
            '瘺' => '瘘',
            '盧' => '卢',
            '瞞' => '瞒',
            '磚' => '砖',
            '磧' => '碛',
            '禦' => '御',
            '積' => '积',
            '穎' => '颖',
            '穌' => '稣',
            '窺' => '窥',
            '簑' => '蓑',
            '築' => '筑',
            '篤' => '笃',
            '篛' => '箬',
            '篩' => '筛',
            '縊' => '缢',
            '縑' => '缣',
            '縈' => '萦',
            '縛' => '缚',
            '縣' => '县',
            '縞' => '缟',
            '縝' => '缜',
            '縉' => '缙',
            '縐' => '绉',
            '膩' => '腻',
            '興' => '兴',
            '艙' => '舱',
            '蕩' => '荡',
            '蕭' => '萧',
            '蕪' => '芜',
            '螞' => '蚂',
            '螢' => '萤',
            '褲' => '裤',
            '親' => '亲',
            '覦' => '觎',
            '諦' => '谛',
            '諺' => '谚',
            '諫' => '谏',
            '諱' => '讳',
            '謀' => '谋',
            '諜' => '谍',
            '諧' => '谐',
            '諮' => '谘',
            '諾' => '诺',
            '謁' => '谒',
            '謂' => '谓',
            '諷' => '讽',
            '諭' => '谕',
            '諳' => '谙',
            '諶' => '谌',
            '諼' => '谖',
            '貓' => '猫',
            '賴' => '赖',
            '踴' => '踊',
            '輻' => '辐',
            '輯' => '辑',
            '輸' => '输',
            '輳' => '辏',
            '辦' => '办',
            '選' => '选',
            '遲' => '迟',
            '遼' => '辽',
            '遺' => '遗',
            '鄴' => '邺',
            '錠' => '锭',
            '錶' => '表',
            '鋸' => '锯',
            '錳' => '锰',
            '錯' => '错',
            '錢' => '钱',
            '鋼' => '钢',
            '錫' => '锡',
            '錄' => '录',
            '錚' => '铮',
            '錐' => '锥',
            '錦' => '锦',
            '錡' => '锜',
            '錕' => '锟',
            '錮' => '锢',
            '錙' => '锱',
            '閻' => '阎',
            '隨' => '随',
            '險' => '险',
            '霑' => '沾',
            '靜' => '静',
            '頰' => '颊',
            '頸' => '颈',
            '頻' => '频',
            '頷' => '颔',
            '頭' => '头',
            '頹' => '颓',
            '頤' => '颐',
            '館' => '馆',
            '餞' => '饯',
            '餛' => '馄',
            '餡' => '馅',
            '餚' => '肴',
            '駭' => '骇',
            '駢' => '骈',
            '駱' => '骆',
            '鬨' => '哄',
            '鮑' => '鲍',
            '鴕' => '鸵',
            '鴣' => '鸪',
            '鴦' => '鸯',
            '鴨' => '鸭',
            '鴒' => '令',
            '鴛' => '鸳',
            '龍' => '龙',
            '龜' => '龟',
            '優' => '优',
            '償' => '偿',
            '儲' => '储',
            '勵' => '励',
            '嚀' => '咛',
            '嚐' => '尝',
            '嚇' => '吓',
            '壓' => '压',
            '壎' => '埙',
            '嬰' => '婴',
            '嬪' => '嫔',
            '嬤' => '嬷',
            '尷' => '尴',
            '屨' => '屦',
            '嶼' => '屿',
            '嶺' => '岭',
            '嶽' => '岳',
            '嶸' => '嵘',
            '幫' => '帮',
            '彌' => '弥',
            '應' => '应',
            '懇' => '恳',
            '戲' => '戏',
            '擊' => '击',
            '擠' => '挤',
            '擰' => '拧',
            '擬' => '拟',
            '擱' => '搁',
            '斂' => '敛',
            '斃' => '毙',
            '曖' => '暧',
            '檔' => '档',
            '檢' => '检',
            '檜' => '桧',
            '櫛' => '栉',
            '檣' => '樯',
            '橾' => '碰',
            '殮' => '殓',
            '氈' => '毡',
            '濘' => '泞',
            '濱' => '滨',
            '濟' => '济',
            '濛' => '蒙',
            '濤' => '涛',
            '濫' => '滥',
            '澀' => '涩',
            '濬' => '浚',
            '濕' => '湿',
            '濰' => '潍',
            '營' => '营',
            '燦' => '灿',
            '燭' => '烛',
            '燬' => '毁',
            '燴' => '烩',
            '牆' => '墙',
            '獰' => '狞',
            '獲' => '获',
            '環' => '环',
            '璦' => '瑷',
            '癆' => '痨',
            '療' => '疗',
            '盪' => '荡',
            '瞭' => '了',
            '矯' => '矫',
            '磯' => '矶',
            '禪' => '禅',
            '簍' => '篓',
            '篠' => '筱',
            '糞' => '粪',
            '糢' => '模',
            '糝' => '糁',
            '縮' => '缩',
            '績' => '绩',
            '繆' => '缪',
            '縷' => '缕',
            '縲' => '缧',
            '繃' => '绷',
            '縫' => '缝',
            '總' => '总',
            '縱' => '纵',
            '繅' => '缫',
            '縴' => '纤',
            '縹' => '缥',
            '繈' => '襁',
            '縵' => '缦',
            '縯' => '演',
            '聲' => '声',
            '聰' => '聪',
            '聯' => '联',
            '聳' => '耸',
            '膿' => '脓',
            '膽' => '胆',
            '臉' => '脸',
            '膾' => '脍',
            '臨' => '临',
            '舉' => '举',
            '艱' => '艰',
            '薑' => '姜',
            '薔' => '蔷',
            '薊' => '蓟',
            '虧' => '亏',
            '螻' => '蝼',
            '蟈' => '蝈',
            '褻' => '亵',
            '褸' => '褛',
            '覬' => '觊',
            '謎' => '谜',
            '謗' => '谤',
            '謙' => '谦',
            '講' => '讲',
            '謊' => '谎',
            '謠' => '谣',
            '謝' => '谢',
            '謄' => '誊',
            '謐' => '谧',
            '谿' => '溪',
            '賺' => '赚',
            '賽' => '赛',
            '購' => '购',
            '賸' => '剩',
            '賻' => '赙',
            '趨' => '趋',
            '轄' => '辖',
            '輾' => '辗',
            '轂' => '毂',
            '轅' => '辕',
            '輿' => '舆',
            '還' => '还',
            '邁' => '迈',
            '醞' => '酝',
            '醜' => '丑',
            '鍍' => '镀',
            '鎂' => '镁',
            '錨' => '锚',
            '鍵' => '键',
            '鍥' => '锲',
            '鍋' => '锅',
            '錘' => '锤',
            '鍾' => '锺',
            '鍬' => '锹',
            '鍛' => '锻',
            '鍰' => '锾',
            '鍚' => '钖',
            '鍔' => '锷',
            '闊' => '阔',
            '闋' => '阕',
            '闌' => '阑',
            '闈' => '闱',
            '闆' => '板',
            '隱' => '隐',
            '隸' => '隶',
            '雖' => '虽',
            '韓' => '韩',
            '顆' => '颗',
            '颶' => '飓',
            '餵' => '喂',
            '騁' => '骋',
            '駿' => '骏',
            '鮮' => '鲜',
            '鮫' => '鲛',
            '鮪' => '鲔',
            '鮭' => '鲑',
            '鴻' => '鸿',
            '鴿' => '鸽',
            '點' => '点',
            '齋' => '斋',
            '叢' => '丛',
            '嚕' => '噜',
            '嚮' => '向',
            '壙' => '圹',
            '壘' => '垒',
            '嬸' => '婶',
            '彞' => '彝',
            '懣' => '懑',
            '擴' => '扩',
            '擲' => '掷',
            '擾' => '扰',
            '攆' => '撵',
            '擺' => '摆',
            '擻' => '擞',
            '擷' => '撷',
            '斷' => '断',
            '檳' => '槟',
            '櫃' => '柜',
            '檻' => '槛',
            '檸' => '柠',
            '櫂' => '棹',
            '檮' => '梼',
            '檯' => '台',
            '歟' => '欤',
            '歸' => '归',
            '殯' => '殡',
            '瀉' => '泻',
            '瀋' => '渖',
            '濾' => '滤',
            '瀆' => '渎',
            '濺' => '溅',
            '瀏' => '浏',
            '燻' => '熏',
            '燼' => '烬',
            '燾' => '焘',
            '獷' => '犷',
            '獵' => '猎',
            '璿' => '璇',
            '甕' => '瓮',
            '癘' => '疠',
            '癒' => '愈',
            '瞼' => '睑',
            '礎' => '础',
            '禮' => '礼',
            '穡' => '穑',
            '穢' => '秽',
            '穠' => '秾',
            '竄' => '窜',
            '竅' => '窍',
            '簫' => '箫',
            '簞' => '箪',
            '簣' => '篑',
            '簡' => '简',
            '糧' => '粮',
            '織' => '织',
            '繕' => '缮',
            '繞' => '绕',
            '繚' => '缭',
            '繡' => '绣',
            '繒' => '缯',
            '繙' => '翻',
            '罈' => '坛',
            '翹' => '翘',
            '職' => '职',
            '聶' => '聂',
            '臍' => '脐',
            '臏' => '膑',
            '舊' => '旧',
            '薩' => '萨',
            '藍' => '蓝',
            '薺' => '荠',
            '薦' => '荐',
            '蟯' => '蛲',
            '蟬' => '蝉',
            '蟲' => '虫',
            '覲' => '觐',
            '觴' => '觞',
            '謨' => '谟',
            '謹' => '谨',
            '謬' => '谬',
            '謫' => '谪',
            '豐' => '丰',
            '贅' => '赘',
            '蹣' => '蹒',
            '蹤' => '踪',
            '蹟' => '迹',
            '蹕' => '跸',
            '軀' => '躯',
            '轉' => '转',
            '轍' => '辙',
            '邇' => '迩',
            '醫' => '医',
            '醬' => '酱',
            '釐' => '厘',
            '鎔' => '熔',
            '鎊' => '镑',
            '鎖' => '锁',
            '鎢' => '钨',
            '鎳' => '镍',
            '鎮' => '镇',
            '鎬' => '镐',
            '鎰' => '镒',
            '鎘' => '镉',
            '鎚' => '锤',
            '鎗' => '枪',
            '闔' => '阖',
            '闖' => '闯',
            '闐' => '阗',
            '闕' => '阙',
            '離' => '离',
            '雜' => '杂',
            '雙' => '双',
            '雛' => '雏',
            '雞' => '鸡',
            '霤' => '溜',
            '鞦' => '秋',
            '額' => '额',
            '顏' => '颜',
            '題' => '题',
            '顎' => '颚',
            '顓' => '颛',
            '颺' => '扬',
            '餾' => '馏',
            '餿' => '馊',
            '餽' => '馈',
            '騎' => '骑',
            '鬆' => '松',
            '魎' => '魉',
            '鯊' => '鲨',
            '鯉' => '鲤',
            '鯽' => '鲫',
            '鯀' => '鲧',
            '鵑' => '鹃',
            '鵝' => '鹅',
            '鵠' => '鹄',
            '儳' => '馋',
            '嚥' => '咽',
            '壞' => '坏',
            '壟' => '垄',
            '壢' => '坜',
            '寵' => '宠',
            '龐' => '庞',
            '廬' => '庐',
            '懲' => '惩',
            '懷' => '怀',
            '懶' => '懒',
            '攏' => '拢',
            '曠' => '旷',
            '櫥' => '橱',
            '櫝' => '椟',
            '櫚' => '榈',
            '櫓' => '橹',
            '瀟' => '潇',
            '瀨' => '濑',
            '瀝' => '沥',
            '瀕' => '濒',
            '瀘' => '泸',
            '爍' => '烁',
            '牘' => '牍',
            '犢' => '犊',
            '獸' => '兽',
            '獺' => '獭',
            '璽' => '玺',
            '瓊' => '琼',
            '疇' => '畴',
            '癟' => '瘪',
            '癡' => '痴',
            '矇' => '蒙',
            '礙' => '碍',
            '禱' => '祷',
            '穫' => '获',
            '穩' => '稳',
            '簾' => '帘',
            '簽' => '签',
            '簷' => '檐',
            '繫' => '系',
            '繭' => '茧',
            '繹' => '绎',
            '繩' => '绳',
            '繪' => '绘',
            '羅' => '罗',
            '繳' => '缴',
            '羶' => '膻',
            '臘' => '腊',
            '藝' => '艺',
            '藪' => '薮',
            '藥' => '药',
            '蟻' => '蚁',
            '蠅' => '蝇',
            '蠍' => '蝎',
            '襠' => '裆',
            '襖' => '袄',
            '譁' => '哗',
            '譜' => '谱',
            '識' => '识',
            '證' => '证',
            '譚' => '谭',
            '譎' => '谲',
            '譏' => '讥',
            '譆' => '嘻',
            '譙' => '谯',
            '贈' => '赠',
            '贊' => '赞',
            '蹺' => '跷',
            '轔' => '辚',
            '轎' => '轿',
            '辭' => '辞',
            '邊' => '边',
            '醱' => '粕',
            '鏡' => '镜',
            '鏑' => '镝',
            '鏟' => '铲',
            '鏃' => '镞',
            '鏈' => '链',
            '鏜' => '镗',
            '鏝' => '镘',
            '鏢' => '镖',
            '鏍' => '镙',
            '鏘' => '锵',
            '鏤' => '镂',
            '鏗' => '铿',
            '鏨' => '錾',
            '關' => '关',
            '隴' => '陇',
            '難' => '难',
            '霧' => '雾',
            '韜' => '韬',
            '韻' => '韵',
            '類' => '类',
            '願' => '愿',
            '顛' => '颠',
            '颼' => '飕',
            '饅' => '馒',
            '饉' => '馑',
            '騖' => '骛',
            '騙' => '骗',
            '鬍' => '胡',
            '鯨' => '鲸',
            '鯧' => '鲳',
            '鯖' => '鲭',
            '鯛' => '鲷',
            '鶉' => '鹑',
            '鵡' => '鹉',
            '鵲' => '鹊',
            '鵪' => '鹌',
            '鵬' => '鹏',
            '麗' => '丽',
            '勸' => '劝',
            '嚨' => '咙',
            '嚶' => '嘤',
            '嚴' => '严',
            '孃' => '娘',
            '寶' => '宝',
            '懸' => '悬',
            '懺' => '忏',
            '攔' => '拦',
            '攙' => '搀',
            '朧' => '胧',
            '櫬' => '榇',
            '瀾' => '澜',
            '瀰' => '弥',
            '瀲' => '潋',
            '爐' => '炉',
            '獻' => '献',
            '瓏' => '珑',
            '癢' => '痒',
            '癥' => '症',
            '礦' => '矿',
            '礪' => '砺',
            '礬' => '矾',
            '礫' => '砾',
            '竇' => '窦',
            '競' => '竞',
            '籌' => '筹',
            '籃' => '篮',
            '糰' => '团',
            '辮' => '辫',
            '繽' => '缤',
            '繼' => '继',
            '罌' => '罂',
            '臚' => '胪',
            '艦' => '舰',
            '藹' => '蔼',
            '藺' => '蔺',
            '蘆' => '芦',
            '蘋' => '苹',
            '蘇' => '苏',
            '蘊' => '蕴',
            '蠔' => '蚝',
            '襤' => '褴',
            '覺' => '觉',
            '觸' => '触',
            '議' => '议',
            '譯' => '译',
            '譟' => '噪',
            '譫' => '谵',
            '贏' => '赢',
            '贍' => '赡',
            '躉' => '趸',
            '躂' => '踏',
            '釋' => '释',
            '鐘' => '钟',
            '鐃' => '铙',
            '鏽' => '锈',
            '闡' => '阐',
            '飄' => '飘',
            '饒' => '饶',
            '饑' => '饥',
            '騫' => '骞',
            '騰' => '腾',
            '騷' => '骚',
            '鰓' => '鳃',
            '鰍' => '鳅',
            '鹹' => '咸',
            '麵' => '面',
            '黨' => '党',
            '齟' => '龃',
            '齣' => '出',
            '齡' => '龄',
            '儷' => '俪',
            '儸' => '罗',
            '囁' => '嗫',
            '囀' => '啭',
            '囂' => '嚣',
            '屬' => '属',
            '懼' => '惧',
            '懾' => '慑',
            '攝' => '摄',
            '攜' => '携',
            '斕' => '斓',
            '櫻' => '樱',
            '欄' => '栏',
            '櫺' => '棂',
            '殲' => '歼',
            '爛' => '烂',
            '犧' => '牺',
            '瓔' => '璎',
            '癩' => '癞',
            '矓' => '胧',
            '籐' => '藤',
            '纏' => '缠',
            '續' => '续',
            '蘗' => '蘖',
            '蘭' => '兰',
            '蘚' => '藓',
            '蠣' => '蛎',
            '蠟' => '蜡',
            '襪' => '袜',
            '覽' => '览',
            '譴' => '谴',
            '護' => '护',
            '譽' => '誉',
            '贓' => '赃',
            '躊' => '踌',
            '躍' => '跃',
            '躋' => '跻',
            '轟' => '轰',
            '辯' => '辩',
            '鐮' => '镰',
            '鐳' => '镭',
            '鐵' => '铁',
            '鐺' => '铛',
            '鐸' => '铎',
            '鐲' => '镯',
            '鐫' => '镌',
            '闢' => '辟',
            '響' => '响',
            '顧' => '顾',
            '顥' => '颢',
            '饗' => '飨',
            '驅' => '驱',
            '驃' => '骠',
            '驀' => '蓦',
            '騾' => '骡',
            '髏' => '髅',
            '鰭' => '鳍',
            '鰥' => '鳏',
            '鶯' => '莺',
            '鶴' => '鹤',
            '鷂' => '鹞',
            '鶸' => '弱',
            '齜' => '龇',
            '齦' => '龈',
            '齧' => '啮',
            '儼' => '俨',
            '儻' => '傥',
            '囈' => '呓',
            '囉' => '罗',
            '孿' => '孪',
            '巔' => '巅',
            '巒' => '峦',
            '彎' => '弯',
            '攤' => '摊',
            '權' => '权',
            '歡' => '欢',
            '灑' => '洒',
            '灘' => '滩',
            '玀' => '猡',
            '疊' => '叠',
            '癮' => '瘾',
            '癬' => '癣',
            '籠' => '笼',
            '籟' => '籁',
            '聾' => '聋',
            '聽' => '听',
            '臟' => '脏',
            '襲' => '袭',
            '襯' => '衬',
            '讀' => '读',
            '贖' => '赎',
            '贗' => '赝',
            '躑' => '踯',
            '躓' => '踬',
            '轡' => '辔',
            '酈' => '郦',
            '鑄' => '铸',
            '鑑' => '鉴',
            '鑒' => '鉴',
            '霽' => '霁',
            '韃' => '鞑',
            '韁' => '缰',
            '顫' => '颤',
            '驕' => '骄',
            '驍' => '骁',
            '髒' => '脏',
            '鬚' => '须',
            '鱉' => '鳖',
            '鰱' => '鲢',
            '鰾' => '鳔',
            '鰻' => '鳗',
            '鷓' => '鹧',
            '鷗' => '鸥',
            '鼴' => '鼹',
            '齬' => '龉',
            '齪' => '龊',
            '龔' => '龚',
            '囌' => '苏',
            '巖' => '岩',
            '戀' => '恋',
            '攣' => '挛',
            '攪' => '搅',
            '曬' => '晒',
            '瓚' => '瓒',
            '竊' => '窃',
            '籤' => '签',
            '籥' => '龠',
            '纓' => '缨',
            '纖' => '纤',
            '纔' => '才',
            '臢' => '臜',
            '蘿' => '萝',
            '蠱' => '蛊',
            '變' => '变',
            '邐' => '逦',
            '邏' => '逻',
            '鑣' => '镳',
            '鑠' => '铄',
            '鑤' => '刨',
            '靨' => '靥',
            '顯' => '显',
            '饜' => '餍',
            '驚' => '惊',
            '驛' => '驿',
            '驗' => '验',
            '體' => '体',
            '鱔' => '鳝',
            '鱗' => '鳞',
            '鱖' => '鳜',
            '鷥' => '鸶',
            '黴' => '霉',
            '囑' => '嘱',
            '壩' => '坝',
            '攬' => '揽',
            '癱' => '瘫',
            '癲' => '癫',
            '羈' => '羁',
            '蠶' => '蚕',
            '讓' => '让',
            '讒' => '谗',
            '讖' => '谶',
            '艷' => '艳',
            '贛' => '赣',
            '釀' => '酿',
            '鑪' => '炉',
            '靂' => '雳',
            '靈' => '灵',
            '靄' => '霭',
            '韆' => '千',
            '顰' => '颦',
            '驟' => '骤',
            '鬢' => '鬓',
            '魘' => '魇',
            '鱟' => '鲎',
            '鷹' => '鹰',
            '鷺' => '鹭',
            '鹼' => '硷',
            '鹽' => '盐',
            '鼇' => '鳌',
            '齷' => '龌',
            '齲' => '龋',
            '廳' => '厅',
            '欖' => '榄',
            '灣' => '湾',
            '籬' => '篱',
            '籮' => '箩',
            '蠻' => '蛮',
            '觀' => '观',
            '躡' => '蹑',
            '釁' => '衅',
            '鑲' => '镶',
            '鑰' => '钥',
            '顱' => '颅',
            '饞' => '馋',
            '髖' => '髋',
            '黌' => '黉',
            '灤' => '滦',
            '矚' => '瞩',
            '讚' => '赞',
            '鑷' => '镊',
            '韉' => '鞯',
            '驢' => '驴',
            '驥' => '骥',
            '纜' => '缆',
            '讜' => '谠',
            '躪' => '躏',
            '釅' => '酽',
            '鑽' => '钻',
            '鑾' => '銮',
            '鑼' => '锣',
            '鱷' => '鳄',
            '鱸' => '鲈',
            '黷' => '黩',
            '豔' => '艳',
            '鑿' => '凿',
            '鸚' => '鹦',
            '驪' => '骊',
            '鬱' => '郁',
            '鸛' => '鹳',
            '鸞' => '鸾',
            '籲' => '吁',
            '兀' => '兀',
            '尒' => '尔',
            '戉' => '钺',
            '肊' => '臆',
            '伝' => '传',
            '匟' => '炕',
            '奼' => '姹',
            '扞' => '捍',
            '扡' => '拖',
            '扠' => '叉',
            '穵' => '挖',
            '艸' => '草',
            '厎' => '砥',
            '抃' => '拚',
            '攷' => '考',
            '杇' => '圬',
            '皁' => '皂',
            '肐' => '胳',
            '芐' => '苄',
            '阨' => '厄',
            '阯' => '址',
            '佪' => '徊',
            '刱' => '创',
            '匊' => '掬',
            '坵' => '丘',
            '怳' => '恍',
            '怌' => '怀',
            '戔' => '戋',
            '抴' => '拽',
            '拑' => '钳',
            '枒' => '丫',
            '杴' => '锨',
            '殀' => '夭',
            '甿' => '氓',
            '疘' => '肛',
            '虯' => '虬',
            '俁' => '俣',
            '剄' => '刭',
            '剉' => '锉',
            '厙' => '厍',
            '咷' => '啕',
            '咼' => '呙',
            '垵' => '埯',
            '垔' => '堙',
            '巹' => '卺',
            '柟' => '楠',
            '毘' => '毗',
            '玅' => '妙',
            '紈' => '纨',
            '胑' => '肢',
            '胊' => '朐',
            '芔' => '卉',
            '釔' => '钇',
            '釓' => '钆',
            '凈' => '净',
            '唚' => '吣',
            '唄' => '呗',
            '唅' => '含',
            '弳' => '弪',
            '捄' => '救',
            '旂' => '旗',
            '栔' => '契',
            '欬' => '咳',
            '毧' => '绒',
            '疿' => '痱',
            '紓' => '纾',
            '郟' => '郏',
            '釕' => '钌',
            '偪' => '逼',
            '剮' => '剐',
            '匭' => '匦',
            '啢' => '喋',
            '圇' => '囵',
            '埳' => '堋',
            '埡' => '垭',
            '婭' => '娅',
            '崠' => '岽',
            '崍' => '崃',
            '淶' => '涞',
            '淥' => '渌',
            '烴' => '烃',
            '眥' => '眦',
            '紺' => '绀',
            '紿' => '绐',
            '脛' => '胫',
            '荳' => '豆',
            '虖' => '宓',
            '袟' => '粗',
            '釤' => '钐',
            '釹' => '钕',
            '釷' => '钍',
            '閆' => '闫',
            '堿' => '碱',
            '媯' => '妫',
            '寑' => '实',
            '崳' => '嵛',
            '惲' => '恽',
            '椏' => '桠',
            '棖' => '枨',
            '殽' => '淆',
            '溈' => '沩',
            '湞' => '浈',
            '牋' => '笺',
            '琖' => '盏',
            '琱' => '雕',
            '畬' => '畲',
            '硤' => '硖',
            '硨' => '砗',
            '絖' => '纩',
            '絏' => '绁',
            '絜' => '洁',
            '絎' => '绗',
            '缾' => '瓶',
            '羢' => '绒',
            '菑' => '灾',
            '衕' => '同',
            '覘' => '觇',
            '觝' => '抵',
            '詎' => '讵',
            '詘' => '诎',
            '詒' => '诒',
            '貺' => '贶',
            '貰' => '贳',
            '軺' => '轺',
            '軹' => '轵',
            '軶' => '轭',
            '軫' => '轸',
            '鄆' => '郓',
            '鈁' => '钫',
            '鈥' => '钬',
            '鈦' => '钛',
            '鈀' => '钯',
            '鈆' => '铅',
            '鈄' => '钭',
            '鈧' => '钪',
            '鈅' => '钥',
            '閌' => '闶',
            '隉' => '陧',
            '雰' => '氛',
            '頇' => '顸',
            '飫' => '饫',
            '僊' => '仙',
            '傴' => '伛',
            '僂' => '偻',
            '僉' => '佥',
            '嗩' => '唢',
            '塤' => '埙',
            '塏' => '垲',
            '嫋' => '袅',
            '媿' => '愧',
            '寘' => '置',
            '尟' => '鲜',
            '巰' => '巯',
            '惷' => '蠢',
            '揫' => '揪',
            '搤' => '扼',
            '摃' => '扛',
            '搨' => '拓',
            '摀' => '捂',
            '搥' => '捶',
            '搧' => '扇',
            '楥' => '楦',
            '煇' => '辉',
            '煒' => '炜',
            '煢' => '茕',
            '猻' => '狲',
            '瑋' => '玮',
            '痾' => '疴',
            '筦' => '管',
            '筭' => '算',
            '筴' => '厕',
            '筩' => '筒',
            '綈' => '绨',
            '綆' => '绠',
            '綃' => '绡',
            '羥' => '羟',
            '耡' => '锄',
            '腡' => '脶',
            '葒' => '荭',
            '葯' => '药',
            '葠' => '参',
            '蛺' => '蛱',
            '裌' => '夹',
            '誆' => '诓',
            '詿' => '诖',
            '詡' => '诩',
            '誄' => '诔',
            '詵' => '诜',
            '跴' => '踩',
            '輅' => '辂',
            '輇' => '辁',
            '遉' => '侦',
            '鄖' => '郧',
            '鄔' => '邬',
            '鉈' => '铊',
            '鈰' => '铈',
            '鈺' => '钰',
            '鉦' => '钲',
            '鈳' => '钶',
            '鉞' => '钺',
            '銃' => '铳',
            '鈮' => '铌',
            '鉆' => '钻',
            '鉭' => '钽',
            '鉬' => '钼',
            '鉏' => '锄',
            '鉲' => '锎',
            '頏' => '颃',
            '頎' => '颀',
            '鳧' => '凫',
            '黽' => '黾',
            '僨' => '偾',
            '僣' => '僭',
            '嘜' => '唛',
            '墑' => '墒',
            '嶁' => '嵝',
            '幘' => '帻',
            '幙' => '幕',
            '廕' => '荫',
            '愬' => '诉',
            '愨' => '悫',
            '慳' => '悭',
            '慴' => '慑',
            '慪' => '怄',
            '戩' => '戬',
            '戧' => '戗',
            '搫' => '搬',
            '摶' => '抟',
            '摳' => '抠',
            '撦' => '扯',
            '摜' => '掼',
            '朢' => '望',
            '榿' => '桤',
            '榪' => '杩',
            '殞' => '殒',
            '滎' => '荥',
            '滸' => '浒',
            '漚' => '沤',
            '漵' => '溆',
            '熗' => '炝',
            '牓' => '榜',
            '獃' => '呆',
            '畽' => '疃',
            '瘖' => '喑',
            '皸' => '皲',
            '碪' => '砧',
            '碭' => '砀',
            '箎' => '篪',
            '劄' => '札',
            '粺' => '稗',
            '綣' => '绻',
            '緄' => '绲',
            '緋' => '绯',
            '綹' => '绺',
            '聝' => '馘',
            '膃' => '腽',
            '蒔' => '莳',
            '蜨' => '蝶',
            '蜺' => '霓',
            '蜑' => '蛋',
            '覡' => '觋',
            '誒' => '诶',
            '誖' => '悖',
            '賕' => '赇',
            '銥' => '铱',
            '鉺' => '铒',
            '銠' => '铑',
            '銪' => '铕',
            '銦' => '铟',
            '銚' => '铫',
            '銫' => '铯',
            '鉿' => '铪',
            '銣' => '铷',
            '鋮' => '铖',
            '銕' => '铁',
            '銩' => '铥',
            '鞀' => '鼗',
            '颮' => '飑',
            '髣' => '仿',
            '儌' => '侥',
            '劌' => '刿',
            '勱' => '劢',
            '嘵' => '哓',
            '噁' => '恶',
            '噉' => '啖',
            '嘸' => '呒',
            '嶗' => '崂',
            '嶠' => '峤',
            '廡' => '庑',
            '憒' => '愦',
            '撣' => '掸',
            '撟' => '挢',
            '撘' => '搭',
            '敺' => '驱',
            '斲' => '斫',
            '槧' => '椠',
            '槼' => '规',
            '毿' => '毵',
            '潁' => '颍',
            '澇' => '涝',
            '潬' => '滩',
            '澂' => '澄',
            '澔' => '浩',
            '潿' => '涠',
            '潷' => '滗',
            '璉' => '琏',
            '瘞' => '瘗',
            '瘨' => '癫',
            '皜' => '颢',
            '窴' => '填',
            '篋' => '箧',
            '緗' => '缃',
            '緡' => '缗',
            '緦' => '缌',
            '緶' => '缏',
            '緱' => '缑',
            '翫' => '玩',
            '舖' => '铺',
            '蓴' => '莼',
            '蔕' => '蒂',
            '蓽' => '荜',
            '蔞' => '蒌',
            '蔦' => '茑',
            '蓯' => '苁',
            '蝟' => '猬',
            '蝯' => '猿',
            '褎' => '袖',
            '諏' => '诹',
            '諑' => '诼',
            '諗' => '谂',
            '賚' => '赉',
            '賝' => '琛',
            '賧' => '赕',
            '鄲' => '郸',
            '醆' => '盏',
            '鋃' => '锒',
            '鋏' => '铗',
            '鋱' => '铽',
            '鋟' => '锓',
            '鋝' => '锊',
            '鋌' => '铤',
            '鋯' => '锆',
            '鋂' => '镅',
            '鋨' => '锇',
            '鋦' => '锔',
            '閬' => '阆',
            '閫' => '阃',
            '靚' => '靓',
            '頦' => '颏',
            '餈' => '糍',
            '餑' => '饽',
            '駔' => '驵',
            '駘' => '骀',
            '魴' => '鲂',
            '鴇' => '鸨',
            '鴈' => '雁',
            '叡' => '睿',
            '噠' => '哒',
            '噦' => '哕',
            '噲' => '哙',
            '嬙' => '嫱',
            '嬡' => '嫒',
            '嶧' => '峄',
            '嶮' => '险',
            '嶨' => '岙',
            '嶴' => '岙',
            '廩' => '廪',
            '懌' => '怿',
            '曏' => '向',
            '橆' => '无',
            '殫' => '殚',
            '澣' => '浣',
            '濇' => '涩',
            '澮' => '浍',
            '燁' => '烨',
            '獧' => '狷',
            '獫' => '猃',
            '獪' => '狯',
            '皻' => '齄',
            '磥' => '磊',
            '磣' => '碜',
            '窶' => '窭',
            '縟' => '缛',
            '縚' => '绦',
            '縋' => '缒',
            '蕓' => '芸',
            '蕘' => '荛',
            '蕆' => '蒇',
            '蕁' => '荨',
            '蕢' => '蒉',
            '蕎' => '荞',
            '蕕' => '莸',
            '薌' => '芗',
            '螘' => '蚁',
            '螄' => '蛳',
            '諠' => '喧',
            '諢' => '诨',
            '謔' => '谑',
            '諤' => '谔',
            '諞' => '谝',
            '踰' => '逾',
            '輷' => '轰',
            '遶' => '绕',
            '鄶' => '郐',
            '錈' => '锩',
            '錟' => '锬',
            '錆' => '锖',
            '鍺' => '锗',
            '錸' => '铼',
            '錛' => '锛',
            '錒' => '锕',
            '錁' => '锞',
            '鍆' => '钔',
            '閼' => '阏',
            '閾' => '阈',
            '閹' => '阉',
            '閶' => '阊',
            '閿' => '阌',
            '閽' => '阍',
            '餧' => '喂',
            '駮' => '驳',
            '骻' => '胯',
            '鮒' => '鲋',
            '鮐' => '鲐',
            '鴝' => '鸲',
            '鴟' => '鸱',
            '嚌' => '哜',
            '嬭' => '奶',
            '幬' => '帱',
            '懃' => '勤',
            '懨' => '恹',
            '懞' => '蒙',
            '擯' => '摈',
            '檁' => '檩',
            '檉' => '柽',
            '歛' => '敛',
            '殭' => '僵',
            '澩' => '泶',
            '濔' => '弥',
            '癉' => '瘅',
            '癇' => '痫',
            '磽' => '硗',
            '簀' => '箦',
            '篳' => '筚',
            '縭' => '缡',
            '縶' => '絷',
            '耬' => '耧',
            '蕷' => '蓣',
            '薙' => '剃',
            '薈' => '荟',
            '薟' => '莶',
            '螾' => '蚓',
            '蟄' => '蛰',
            '褵' => '缡',
            '褳' => '裢',
            '覯' => '觏',
            '謖' => '谡',
            '謅' => '诌',
            '謚' => '谥',
            '蹓' => '遛',
            '蹌' => '跄',
            '鍤' => '锸',
            '鍇' => '锴',
            '鍼' => '针',
            '鍘' => '铡',
            '鍶' => '锶',
            '闇' => '暗',
            '闀' => '哄',
            '闃' => '阒',
            '隮' => '跻',
            '餬' => '糊',
            '餳' => '饧',
            '餱' => '糇',
            '騃' => '呆',
            '骾' => '鲠',
            '鮚' => '鲒',
            '鮞' => '鲕',
            '鴯' => '鸸',
            '鴰' => '鸹',
            '鵂' => '鸺',
            '鵃' => '鸼',
            '黿' => '鼋',
            '齔' => '龀',
            '嚙' => '啮',
            '懟' => '怼',
            '擿' => '掷',
            '攄' => '摅',
            '擼' => '撸',
            '毉' => '医',
            '瀅' => '滢',
            '瀦' => '潴',
            '濼' => '泺',
            '燿' => '耀',
            '癤' => '疖',
            '簰' => '箅',
            '繖' => '伞',
            '繢' => '缋',
            '聵' => '聩',
            '藎' => '荩',
            '蟣' => '虮',
            '襆' => '幞',
            '襉' => '裥',
            '謳' => '讴',
            '謼' => '呼',
            '謾' => '谩',
            '賾' => '赜',
            '贄' => '贽',
            '蹠' => '跖',
            '蹧' => '糟',
            '蹝' => '屣',
            '轆' => '辘',
            '鄺' => '邝',
            '鎵' => '镓',
            '鎌' => '镰',
            '鎒' => '耨',
            '鎝' => '锝',
            '鎧' => '铠',
            '鎪' => '锼',
            '鎦' => '镏',
            '韙' => '韪',
            '餼' => '饩',
            '騏' => '骐',
            '騍' => '骒',
            '騅' => '骓',
            '鬩' => '阋',
            '鯇' => '鲩',
            '鯁' => '鲠',
            '鵜' => '鹈',
            '鵓' => '鹁',
            '鵒' => '鹆',
            '厴' => '厣',
            '嚦' => '呖',
            '嚪' => '啖',
            '嚬' => '频',
            '壚' => '垆',
            '櫧' => '槠',
            '櫟' => '栎',
            '櫫' => '橥',
            '櫞' => '橼',
            '氌' => '氇',
            '瀧' => '泷',
            '瀠' => '潆',
            '甖' => '罂',
            '礡' => '礴',
            '禰' => '祢',
            '穨' => '颓',
            '繰' => '缲',
            '繯' => '缳',
            '罋' => '瓮',
            '羃' => '幂',
            '羆' => '罴',
            '臕' => '膘',
            '艤' => '舣',
            '蟺' => '蟮',
            '蟶' => '蛏',
            '蠆' => '虿',
            '襢' => '袒',
            '襝' => '裣',
            '覈' => '核',
            '覷' => '觑',
            '觶' => '觯',
            '譈' => '憝',
            '譖' => '谮',
            '譔' => '撰',
            '譋' => '谰',
            '鏞' => '镛',
            '鏇' => '镟',
            '鏹' => '镪',
            '鏬' => '罅',
            '鏌' => '镆',
            '鎩' => '铩',
            '韝' => '鞲',
            '韞' => '韫',
            '顙' => '颡',
            '饈' => '馐',
            '饇' => '饫',
            '饃' => '馍',
            '騣' => '鬃',
            '鯪' => '鲮',
            '鯫' => '鲰',
            '鯤' => '鲲',
            '鯢' => '鲵',
            '鯰' => '鲶',
            '鯔' => '鲻',
            '鯗' => '鲞',
            '鯡' => '鲱',
            '鵰' => '雕',
            '鵯' => '鹎',
            '鶇' => '鸫',
            '嚳' => '喾',
            '攖' => '撄',
            '櫳' => '栊',
            '櫪' => '枥',
            '櫨' => '栌',
            '獼' => '猕',
            '穭' => '稆',
            '繾' => '缱',
            '聹' => '聍',
            '臙' => '胭',
            '蘢' => '茏',
            '藶' => '苈',
            '蘄' => '蕲',
            '蠐' => '蛴',
            '蠑' => '蝾',
            '譭' => '毁',
            '趮' => '躁',
            '鐋' => '铴',
            '鐓' => '镦',
            '鐠' => '镨',
            '鐔' => '镡',
            '鐐' => '镣',
            '鐨' => '镄',
            '鐙' => '镫',
            '鏵' => '铧',
            '鐀' => '柜',
            '鏷' => '镤',
            '鐒' => '铹',
            '闞' => '阚',
            '顢' => '颟',
            '饌' => '馔',
            '饋' => '馈',
            '騶' => '驺',
            '騮' => '骝',
            '騸' => '骟',
            '騭' => '骘',
            '鰈' => '鲽',
            '鰒' => '鳆',
            '鰉' => '鳇',
            '鶘' => '鹕',
            '鶚' => '鹗',
            '鶿' => '鹚',
            '鶩' => '鹜',
            '齠' => '龆',
            '齙' => '龅',
            '儺' => '傩',
            '儹' => '攒',
            '巋' => '岿',
            '懽' => '欢',
            '攛' => '撺',
            '櫸' => '榉',
            '灃' => '沣',
            '灄' => '滠',
            '礱' => '砻',
            '糲' => '粝',
            '纊' => '纩',
            '纈' => '缬',
            '纍' => '累',
            '臝' => '裸',
            '蘞' => '蔹',
            '衊' => '蔑',
            '贐' => '赆',
            '轝' => '舆',
            '鐿' => '镱',
            '鐶' => '钚',
            '鑀' => '锿',
            '闥' => '闼',
            '飆' => '飙',
            '驄' => '骢',
            '驂' => '骖',
            '驁' => '骜',
            '鰣' => '鲥',
            '鰨' => '鳎',
            '鰩' => '鳐',
            '鶼' => '鹣',
            '鶻' => '鹘',
            '鹺' => '鹾',
            '齎' => '赍',
            '囅' => '冁',
            '孌' => '娈',
            '攢' => '攒',
            '灕' => '漓',
            '癭' => '瘿',
            '籜' => '箨',
            '糴' => '籴',
            '罏' => '垆',
            '艫' => '舻',
            '覿' => '觌',
            '譾' => '谫',
            '讅' => '审',
            '躕' => '蹰',
            '躚' => '跹',
            '躒' => '跞',
            '轢' => '轹',
            '鑌' => '镔',
            '鑊' => '镬',
            '驏' => '骣',
            '驊' => '骅',
            '鱈' => '鳕',
            '鰹' => '鲣',
            '鰳' => '鳓',
            '鰼' => '鳛',
            '鰷' => '鲦',
            '鰲' => '鳌',
            '鷚' => '鹨',
            '鷙' => '鸷',
            '龕' => '龛',
            '龢' => '和',
            '攩' => '挡',
            '欑' => '攒',
            '欒' => '栾',
            '欏' => '椤',
            '玁' => '猃',
            '癰' => '痈',
            '蘺' => '蓠',
            '讌' => '宴',
            '讎' => '雠',
            '轤' => '轳',
            '鱒' => '鳟',
            '鱘' => '鲟',
            '鱍' => '鲅',
            '鷯' => '鹩',
            '鷸' => '鹬',
            '鷦' => '鹪',
            '鷲' => '鹫',
            '鷰' => '燕',
            '鷳' => '鹇',
            '黲' => '黪',
            '齏' => '齑',
            '囓' => '啮',
            '灝' => '灏',
            '灠' => '漤',
            '籪' => '簖',
            '讕' => '谰',
            '髕' => '髌',
            '鱣' => '鳝',
            '鱧' => '鳢',
            '鱠' => '脍',
            '齶' => '腭',
            '矙' => '瞰',
            '籩' => '笾',
            '糶' => '粜',
            '纘' => '缵',
            '臠' => '脔',
            '讙' => '欢',
            '躥' => '蹿',
            '鑭' => '镧',
            '饟' => '饷',
            '鱭' => '鲚',
            '鼉' => '鼍',
            '趲' => '趱',
            '躦' => '躜',
            '釃' => '酾',
            '鑵' => '罐',
            '龤' => '谐',
            '灨' => '赣',
            '讞' => '谳',
            '顳' => '颞',
            '顴' => '颧',
            '驤' => '骧',
            '驦' => '骦',
            '鸕' => '鸬',
            '戇' => '戆',
            '欞' => '棂',
            '钂' => '镋',
            '钁' => '镢',
            '鬮' => '阄',
            '驫' => '骉',
            '鱺' => '鲡',
            '鸝' => '鹂',
            '灩' => '滟',
            '麤' => '粗',
            '銹' => '锈',
            '裏' => '里',
            '墻' => '墙',
            '粧' => '妆',
            '嫺' => '娴',
            'Ⅰ' => '1', 'Ⅱ' => '2', 'Ⅲ' => '3', 'Ⅳ' => '4', 'Ⅴ' => '5', 'Ⅵ' => '6', 'Ⅶ' => '7', 'Ⅷ' => '8', 'Ⅸ' => '9', 'Ⅹ' => '10',

            '°' => '度', '℃' => '摄氏度',
        );
        return strtr($word, $arr);
    }
    private $segmentPinyin = false;

    public function onPinyin()
    {
        $this->segmentPinyin = true;
    }

    public function closePinyin()
    {
        $this->segmentPinyin = true;
    }
    private $isStringPreprocessing = false;

    public function onStringPreprocessing()
    {
        $this->isStringPreprocessing = true;
    }

    private $isTraditionalToSimplified = false;
    public function onTraditionalToSimplified()
    {
        $this->isTraditionalToSimplified = true;
    }
    private $isZhToNumber = false;

    public function onZhToNumber()
    {
        $this->isZhToNumber = true;
    }

    public function segment($str, $is_all = false, $idf = false)
    {
        $str = strtolower($str);
        if ($this->isStringPreprocessing) {
            $str = $this->stringPreprocessing($str);
        }
        if ($this->isTraditionalToSimplified) {
            $str = $this->traditionalToSimplified($str);
        }

        if ($is_all) {
            $arrresult = $this->fcHandle->segmentAll($str);
        } else {
            $arrresult = $this->fcHandle->standard($str, $idf);
        }
        if ($idf) {
            $idf = isset($arrresult['idf']) ? $arrresult['idf'] : [];
            $this->idfContainer = $idf;
            $arrresult = $arrresult['terms'];
        }
        if ($this->isZhToNumber) {
            $ZhToNumber = $this->ZhToNumber($str);
            $arrresult = array_merge($arrresult, $ZhToNumber);
        }
        if (!empty($arrresult)) {
            $arrresult = array_unique($arrresult);
            $arrresult = array_filter($arrresult);
            $arrresult = array_values($arrresult);
        }
        return $arrresult;
    }

    public function nGram($str, $len = 3)
    {
        if (is_array($len)) {
            $len[0] = (int)$len[0];
            $len[1] = (int)$len[1];
        } else {
            $len = (int)$len;
            $len = ($len > 0) ? ceil($len) : 1;
        }
        $str = strtolower($str);
        if ($this->isStringPreprocessing) {
            $str = $this->stringPreprocessing($str);
        }
        if ($this->isTraditionalToSimplified) {
            $str = $this->traditionalToSimplified($str);
        }
        $arrresult = $this->fcHandle->segmentNgram($str, $len);
        if ($this->isZhToNumber) {
            $ZhToNumber = $this->ZhToNumber($str);
            $arrresult = array_merge($arrresult, $ZhToNumber);
        }
        if (!empty($arrresult)) {
            $arrresult = array_unique($arrresult);
            $arrresult = array_filter($arrresult);
            $arrresult = array_values($arrresult);
        }
        return $arrresult;
    }

    private function getFirstLetter($str)
    {
        $arr_fh = array(
            33 => 'A', 34 => 'B', 35 => 'C', 36 => 'D', 37 => 'E', 38 => 'F', 39 => 'G', 40 => 'H', 41 => 'I', 42 => 'J', 43 => 'K', 44 => 'L', 45 => 'M', 46 => 'N', 47 => 'O',             48 => 'H', 49 => 'I', 50 => 'J', 51 => 'K', 52 => 'L', 53 => 'M', 54 => 'N', 55 => 'O', 56 => 'P', 57 => 'Q',             58 => 'A', 59 => 'B', 60 => 'C', 61 => 'D', 62 => 'E', 63 => 'F', 64 => 'G',
            91 => 'P', 92 => 'Q', 93 => 'R', 94 => 'S', 95 => 'T', 96 => 'V',             123 => 'W', 124 => 'X', 125 => 'Y', 126 => 'Z',
        );
        $offsetStr = isset($str[0]) ? $str[0] : '';
        $fchar = ord($offsetStr);
        if (($fchar >= 65 && $fchar <= 90) || ($fchar >= 97 && $fchar <= 122)) {
            return strtoupper($str[0]);
        } else if (($fchar >= 33 && $fchar < 64) || ($fchar >= 91 && $fchar < 96) || ($fchar >= 123 && $fchar <= 126)) {
            $zm = $arr_fh[$fchar];
            return strtoupper($zm);
        } else {
            $s1 = mb_convert_encoding($str, "GBK", "UTF-8");
            $s2 = mb_convert_encoding($s1, "UTF-8", "GBK");
            $s = ($s2 == $str) ? $s1 : $str;


            $asc = ord((isset($s[0]) ? $s[0] : '')) * 256 + ord((isset($s[1]) ? $s[1] : '')) - 65536;
            if ($asc >= -20319 && $asc <= -20284) {
                return 'A';
            }
            if ($asc >= -20283 && $asc <= -19776) {
                return 'B';
            }
            if ($asc >= -19775 && $asc <= -19219) {
                return 'C';
            }
            if ($asc >= -19218 && $asc <= -18711) {
                return 'D';
            }
            if ($asc >= -18710 && $asc <= -18527) {
                return 'E';
            }
            if ($asc >= -18526 && $asc <= -18240) {
                return 'F';
            }
            if ($asc >= -18269 && $asc <= -17923) {
                return 'G';
            }
            if ($asc >= -17922 && $asc <= -17418) {
                return 'H';
            }
            if ($asc >= -17417 && $asc <= -16475) {
                return 'J';
            }
            if ($asc >= -16474 && $asc <= -16213) {
                return 'K';
            }
            if ($asc >= -16212 && $asc <= -15641) {
                return 'L';
            }
            if ($asc >= -15640 && $asc <= -15166) {
                return 'M';
            }
            if ($asc >= -15165 && $asc <= -14923) {
                return 'N';
            }
            if ($asc >= -14922 && $asc <= -14915) {
                return 'O';
            }
            if ($asc >= -14914 && $asc <= -14631) {
                return 'P';
            }
            if ($asc >= -14630 && $asc <= -14150) {
                return 'Q';
            }
            if ($asc >= -14149 && $asc <= -14091) {
                return 'R';
            }
            if ($asc >= -14090 && $asc <= -13319) {
                return 'S';
            }
            if ($asc >= -13318 && $asc <= -12839) {
                return 'T';
            }
            if ($asc >= -12838 && $asc <= -12557) {
                return 'W';
            }
            if ($asc >= -12556 && $asc <= -11848) {
                return 'X';
            }
            if ($asc >= -11847 && $asc <= -11056) {
                return 'Y';
            }
            if ($asc >= -11055 && $asc <= -10247) {
                return 'Z';
            } else {
                return 'U';
            }
        }
    }

    private function AngleToRadian($latitude, $longitude)
    {
        $radLat = deg2rad($latitude);
        $radLon = deg2rad($longitude);
        return $radLat . ',' . $radLon;
    }

    public function buildIndexInit()
    {
        $this->loadHeightFreqWord();
        $this->loadSymbolStopword();
        $this->geohash();
    }

    public function destroyDic()
    {
        $this->fcHandle->destroyDic();
    }
    private $geohashObj = false;
    private function geohash()
    {
        $this->geohashObj = new Geohash;
    }
    public $realTimeIndexTemp = [];
    private $latlonBase32Container = [];
    private $importLog = [];
    private $autoCompletionPrefixContainer = [];
    private $countNumericMinMax = [];
    private $numericContainer = [];
    private $dateContainer = [];

    public function indexer($rowData)
    {
        $primarykey = $this->mapping['properties']['primarykey'];
        if (!isset($rowData[$primarykey])) {
            return $this->getError(1);
        }
        if ($this->primarykeyType === 'Int_Incremental') {
            $id = (int)$rowData[$primarykey];
        } else {
            $id = $rowData[$primarykey];
        }
        if ($id === 0) {
            return $this->getError(2);
        }
        if (empty($this->mapping)) {
            return false;
        }
        ++$this->countId;
        $rowDataTemp = [];
        $allFieldName = (array)array_column($this->mapping['properties']['all_field'], 'name');
        $allFieldType = (array)$this->mapping['properties']['allFieldType'];
        foreach ($rowData as $fd => $d) {
            if (in_array($fd, $allFieldName)) {
                if (in_array($fd, $allFieldType['numeric'])) {
                    $d = (float)$d;
                    $this->numericContainer[$fd][$id] = $d;
                } else if (in_array($fd, $allFieldType['date'])) {
                    if ($this->isValidDateString($d)) {
                        $date = strtotime($d);
                        if ($date) {
                            $d = $date;
                        } else {
                            $d = 0;
                        }
                    } else {
                        $d = (int)$d;
                    }
                    $this->dateContainer[$fd][$id] = date('Y-m-d', $d);
                }
                $rowDataTemp[$fd] = $d;
            }
        }
        if ((count($rowDataTemp) != count($allFieldName))) {
            return $this->getError(3);
        }
        $this->dataTemp[] = $rowDataTemp;
        $fc_string = '';
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = isset($fd['name']) ? $fd['name'] : '';
            $isAutoCompletion = isset($this->mapping['properties']['auto_completion'][$field]);
            $type = isset($fd['type']) ? $fd['type'] : '';
            $analyzer = isset($fd['analyzer']) ? $fd['analyzer'] : '';
            $fc_string = isset($rowData[$field]) ? $rowData[$field] : '';
            if ($fc_string == '') {
                continue;
            }
            if ($type == 'geo_point') {
                if (!is_array($this->geoRadianContair[$field])) {
                    $this->geoRadianContair[$field] = [];
                }
                if (!is_array($this->latlonBase32Container[$field])) {
                    $this->latlonBase32Container[$field] = [];
                }
                if (is_array($fc_string) && isset($fc_string['latitude']) && isset($fc_string['longitude'])) {
                    $fc_string['latitude'] = (float)$fc_string['latitude'];
                    $fc_string['longitude'] = (float)$fc_string['longitude'];
                    $radian = $this->AngleToRadian($fc_string['latitude'], $fc_string['longitude']);
                } else {
                    list($lat, $lon) = explode(',', $fc_string);
                    $lat = (float)$lat;
                    $lon = (float)$lon;
                    $radian = $this->AngleToRadian($lat, $lon);
                }
                $base32 = $this->geohashObj::encode($lat, $lon, 12);
                $this->latlonBase32Container[$field][] =  $id . ' ' . $base32;
                $this->geoRadianContair[$field][] = $id . ' ' . $radian;
                if ($this->realTimeIndex) {
                    $this->realTimeIndexTemp[$field][] = [
                        'id' => $id,
                        'radian' => $radian,
                    ];
                }
                continue;
            } else if ($type == 'text') {
                $fc_arr = $this->analyzerEntry($fc_string, $analyzer, true, false);
                $fc_arr = $this->filterSymbolStopWord($fc_arr);
                if ($this->segmentPinyin) {
                    $fc_arr_str = implode(' ', $fc_arr);
                    $regx_zh = '([\x{4e00}-\x{9fa5}]+)';
                    if (preg_match_all('/' . $regx_zh . '/u', $fc_arr_str, $mat)) {
                        if (!empty($mat[0])) {
                            $pinyin = $this->getPinyin($mat[0]);
                            if (!empty($pinyin)) {
                                $fc_arr = array_merge($fc_arr, $pinyin['firstChar'], $pinyin['pinyin']);
                            }
                        }
                    }
                }
            } else if ($type == 'keyword') {
                $fc_arr = [str_replace(' ', '', $fc_string)];
            } else if ($type == 'numeric') {
                $num = (float)$fc_string;
                $fc_arr = [$num];
            } else if ($type == 'date') {
                if ($this->isValidDateString($fc_string)) {
                    $date = strtotime($fc_string);
                    if ($date) {
                        $fc_arr = [$date];
                    } else {
                        return $this->getError(5, $fc_string);
                    }
                } else {
                    $fc_arr = [(int)$fc_string];
                }
            } else {
                return $this->getError(4);
            }


            if ($this->realTimeIndex) {
                $this->realTimeIndexTemp[$field][] = [
                    'id' => $id,
                    'terms' => $fc_arr,
                ];
            }
            foreach ($fc_arr as $c => $v) {
                $fir = $this->getFirstLetter($v);


                $this->segword[$field][$fir][] = $v . ' ' . $id . PHP_EOL;
            }
        }
        if (!empty($this->mapping['properties']['auto_completion_field'])) {
            foreach ($this->mapping['properties']['auto_completion_field'] as $fd) {
                $fc_string = isset($rowData[$fd]) ? $rowData[$fd] : '';
                $this->cutPrefix($fc_string, $this->autoCompletePrefixLen, $fd);
            }
        }
        return true;
    }
    private function cutPrefix($str, $len, $field)
    {
        if (!isset($this->autoCompletionPrefixContainer[$field])) {
            $this->autoCompletionPrefixContainer[$field] = [];
        }
        $replace = [
            ' ' => '',
            PHP_EOL => '',
        ];
        $str = trim(strtr($str, $replace));
        $str = preg_replace("/\s+|\|/", "", $str);
        $autoCompletionStr = strtolower($str);
        $strlen = mb_strlen($autoCompletionStr);
        $maxlen = ($strlen > ($len - 1)) ? $len : $strlen;
        for ($i = 1; $i <= $maxlen; $i++) {
            $sub = mb_substr($autoCompletionStr, 0, $i);
            if ($sub) {
                $this->autoCompletionPrefixContainer[$field][$sub][] = $str;
            }
        }
    }
    private $firstCharObject = false;
    private $pinyinObject = false;

    private function getPinyin($words = [])
    {
        if ($words == '' || empty($words)) {
            return [];
        }
        $string = implode(' ', $words);
        if (!$this->firstCharObject) {
            $this->firstCharObject = new \WindSearch\Core\ZhToFirstChar();
        }
        $firstChar = $this->firstCharObject->Pinyin($string);
        $firstChar = strtolower($firstChar);
        if (!$this->pinyinObject) {
            $this->pinyinObject = new \WindSearch\Core\ZhToPinyin();
        }
        $pinyin = $this->pinyinObject->Pinyin($string);
        $res = [
            'firstChar' => explode(' ', $firstChar),
            'pinyin' => explode(' ', $pinyin),
        ];
        return $res;
    }

    private function initSqlite()
    {
        $dir = $this->getStorageDir();
        if (!is_dir($dir . 'baseData/')) {
            mkdir($dir . 'baseData/', 0777);
        }
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        if (is_file($dir)) {
            unlink($dir);
        }

        $pdo = new PDO_sqlite($dir);
        $pdo->init($this->IndexName, $this->primarykey, $this->primarykeyType);
    }
    private $interval_mapping = false;

    public function delete($primarykey)
    {
        $file = $this->indexDir . $this->IndexName . '/index/delete/delete_primarykey_list.txt';
        if (is_file($file)) {
            $delete_list = (array)json_decode(file_get_contents($file), true);
        } else {
            $delete_list = [];
        }
        $delete_list[] = $primarykey;
        self::del_dir(dirname(__FILE__) . '/cache/');
        return file_put_contents($file, json_encode(array_unique($delete_list)));
    }

    public function restore($primarykey)
    {
        $file = $this->indexDir . $this->IndexName . '/index/delete/delete_primarykey_list.txt';
        if (is_file($file)) {
            $delete_list = (array)json_decode(file_get_contents($file), true);
        } else {
            $delete_list = [];
        }
        $find = array_search($primarykey, $delete_list);
        if ($find !== false) {
            unset($delete_list[$find]);
        }
        return file_put_contents($file, json_encode(array_unique($delete_list)));
    }

    private function get_delete_privmarykey()
    {
        $file = $this->indexDir . $this->IndexName . '/index/delete/delete_primarykey_list.txt';
        if (is_file($file)) {
            return (array)json_decode(file_get_contents($file), true);
        } else {
            return [];
        }
    }

    private function getNumericInterval($num, $fd)
    {
        $num = (float)$num;
        if (!$this->interval_mapping[$fd]) {
            $this->interval_mapping[$fd] = json_decode(file_get_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $fd . '_interval_mapping.txt'), true);
        }
        if (is_array($this->interval_mapping[$fd])) {
            $left = 0;
            $right = count($this->interval_mapping[$fd]) - 2;
            while ($left <= $right) {
                $mid = intdiv(($left + $right), 2);
                if ($this->interval_mapping[$fd][$mid] <= $num && $num < $this->interval_mapping[$fd][$mid + 1]) {
                    return $mid;
                } else if ($num < $this->interval_mapping[$fd][$mid]) {
                    $right = $mid - 1;
                } else {
                    $left = $mid + 1;
                }
            }
            if ($num < $this->interval_mapping[$fd][0]) {
                return 'l';
            } else if ($num >= end($this->interval_mapping[$fd])) {
                return 'r';
            }
            return false;
        }
    }

    private function getDateNYR($timestamp)
    {
        $interval = (float)date('Ymd', $timestamp);
        return $interval;
    }







    private function getDateIntervalData($fd)
    {
        if (!$this->interval_mapping[$fd]) {
            $this->interval_mapping[$fd] = json_decode(file_get_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $fd . '_interval_mapping.txt'), true);
        }
    }

    public function batchWrite()
    {
        if (empty($this->mapping)) {
            return;
        }
        $flock = fopen(dirname(__FILE__) . '/windIndexCore/fileLock/batchWrite', 'w+b');
        flock($flock, LOCK_SH);
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        $pdo = new PDO_sqlite($dir);
        $primarykey = $this->mapping['properties']['primarykey'];
        $pdo->beginTransaction();
        foreach ($this->dataTemp as $v) {
            $id = $v[$primarykey];
            $cont = $this->systemCompression(json_encode($v));
            $sql = "INSERT INTO $this->IndexName ($primarykey, doc) VALUES ('$id','$cont');";
            $pdo->exec($sql);
        }
        $pdo->commit();
        $this->dataTemp = [];
        $allFieldType = (array)$this->mapping['properties']['allFieldType'];
        if (!empty($this->numericContainer)) {
            $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
            foreach ($this->numericContainer as $fd => $list) {
                $temp = [];
                foreach ($list as $d => $num) {
                    $temp[] = $d . ' ' . $num;
                }
                file_put_contents($indexSegDir . $fd . '/interval/list.txt', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
            }
        }
        if (!empty($this->dateContainer)) {
            $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
            foreach ($this->dateContainer as $fd => $list) {
                $temp = [];
                foreach ($list as $d => $num) {
                    $num = str_replace('-', '', $num);
                    $temp[$num][] = $d;
                }
                foreach ($temp as $i => $l) {
                    file_put_contents($indexSegDir . $fd . '/interval/' . $i . '.txt', implode(PHP_EOL, $l) . PHP_EOL, FILE_APPEND);
                }
            }
        }
        $zm = array(
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'
        );
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            $type = isset($fd['type']) ? $fd['type'] : '';
            if ($type == 'geo_point') {
                $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
                if (isset($this->latlonBase32Container[$field]) && !empty($this->latlonBase32Container[$field])) {
                    $open = fopen($indexSegDir . $field . '/terms/geo_point_base32.index', "a");
                    fwrite($open, implode(PHP_EOL, $this->latlonBase32Container[$field]) . PHP_EOL);
                }
                if (isset($this->geoRadianContair[$field]) && !empty($this->geoRadianContair[$field])) {
                    $open = fopen($indexSegDir . $field . '/terms/geo_point_radian.index', "a");
                    fwrite($open, implode(PHP_EOL, $this->geoRadianContair[$field]) . PHP_EOL);
                }
                if (isset($this->realTimeIndexTemp[$field]) && $this->realTimeIndex) {
                    $this->real_time_index_geo_point($field,  $this->realTimeIndexTemp[$field]);
                }
            } else {
                if (isset($this->realTimeIndexTemp[$field]) && $this->realTimeIndex) {
                    foreach ($this->realTimeIndexTemp[$field] as $f) {
                        if ($this->primarykeyType == 'Int_Incremental') {
                            if ($type === 'keyword') {
                                $this->real_time_index_postlist($field,  $f['terms'], $f['id']);
                            } else {
                                $this->real_time_index_bitmap($field, $f['terms'], $f['id']);
                            }
                        } else {
                            $this->real_time_index_postlist($field,  $f['terms'], $f['id']);
                        }
                        $this->real_time_index_storage_terms($field, $f['terms']);
                    }
                }
                $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/';
                foreach ($zm as $v) {
                    if (isset($this->segword[$field][$v])) {
                        $open = fopen($indexSegDir . $field . '/terms/' . $v . '.index', "a");
                        fwrite($open, implode('', $this->segword[$field][$v]));
                        fclose($open);
                    }
                }
            }
        }
        if (!empty($this->mapping['properties']['auto_completion_field'])) {
            foreach ($this->mapping['properties']['auto_completion_field'] as $field) {
                $indexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIndexSeg/' . $field . '/';
                if (!is_dir($indexSegDir)) {
                    mkdir($indexSegDir, 0777, true);
                }
                $temp = [];
                foreach ($this->autoCompletionPrefixContainer[$field] as $k => $v) {
                    $firstChar = $this->getFirstLetter($k);
                    $temp[$firstChar][] = $k . ' ' . json_encode($v) . PHP_EOL;
                }
                foreach ($temp as $firstChar => $v) {
                    file_put_contents($indexSegDir . '/' . $firstChar . '.index', implode('', $v), FILE_APPEND);
                }
            }
        }
        if ($this->realTimeIndex) {
            $this->realtimeIndexNumericAndDate();
        }
        if ($this->realTimeIndex) {
            $this->realTimeIndexBatchWrite();
        }
        if ($this->isIncrIndex) {
            foreach ($this->mapping['properties']['field'] as $fd) {
                $field = $fd['name'];
                $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/terms/';
                $type = isset($fd['type']) ? $fd['type'] : '';
                if ($type == 'geo_point') {
                    if (isset($this->latlonBase32Container[$field]) && !empty($this->latlonBase32Container[$field])) {
                        $open = fopen($incrementIndexSegDir . 'geo_point.index', "a");
                        fwrite($open, implode(PHP_EOL, $this->latlonBase32Container[$field]) . PHP_EOL);
                    }
                    if (isset($this->geoRadianContair[$field]) && !empty($this->geoRadianContair[$field])) {
                        $open = fopen($incrementIndexSegDir . 'geo_point_radian.index', "a");
                        fwrite($open, implode(PHP_EOL, $this->geoRadianContair[$field]) . PHP_EOL);
                    }
                } else {


                    foreach ($zm as $v) {
                        if (isset($this->segword[$field][$v])) {
                            $open = fopen($incrementIndexSegDir . $v . '.index', "a");
                            fwrite($open, implode('', $this->segword[$field][$v]));
                            fclose($open);
                        }
                    }
                }
            }
            if (!empty($this->mapping['properties']['auto_completion_field'])) {
                foreach ($this->mapping['properties']['auto_completion_field'] as $field) {
                    $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/' . $field . '/';
                    if (!is_dir($incrementIndexSegDir)) {
                        mkdir($incrementIndexSegDir, 0777, true);
                    }
                    $temp = [];
                    foreach ($this->autoCompletionPrefixContainer[$field] as $k => $v) {
                        $firstChar = $this->getFirstLetter($k);
                        $temp[$firstChar][] = $k . ' ' . json_encode($v) . PHP_EOL;
                    }
                    foreach ($temp as $firstChar => $v) {
                        file_put_contents($incrementIndexSegDir . '/' . $firstChar . '.index', implode('', $v), FILE_APPEND);
                    }
                }
            }
            if (!empty($this->numericContainer)) {
                $indexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
                foreach ($this->numericContainer as $fd => $list) {
                    $temp = [];
                    foreach ($list as $d => $num) {
                        $temp[] = $d . ' ' . $num;
                    }
                    file_put_contents($indexSegDir . $fd . '/interval/list.txt', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                }
                unset($this->numericContainer);
                unset($temp);
            }
            if (!empty($this->dateContainer)) {
                $indexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
                foreach ($this->dateContainer as $fd => $list) {
                    $temp = [];
                    foreach ($list as $d => $num) {
                        $temp[$num][] = $d;
                    }
                    foreach ($temp as $i => $l) {
                        file_put_contents($indexSegDir . $fd . '/interval/' . $i . '.txt', implode(PHP_EOL, $l) . PHP_EOL, FILE_APPEND);
                    }
                }
                unset($this->dateContainer);
            }
        }
        $countIdFile = $this->indexDir . $this->IndexName . '/summarizedData/countIndex';
        $oldIdCount = (int)file_get_contents($countIdFile);
        $newIdCount = $this->countId;
        $totalId = $oldIdCount + $newIdCount;
        file_put_contents($countIdFile, $totalId);
        $countIdFile = $this->indexDir . $this->IndexName . '/summarizedData/unIndexCount';
        $oldIdCount = (int)file_get_contents($countIdFile);
        $newIdCount = $this->countId;
        $totalId = $oldIdCount + $newIdCount;
        file_put_contents($countIdFile, $totalId);
        $this->segword = [];
        $this->latlonBase32Container = [];
        $this->geoRadianContair = [];
        $this->realTimeIndexTemp = [];
        $this->autoCompletionPrefixContainer = [];
        flock($flock, LOCK_UN);
    }


    private function buildPostingListWholeIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', '');
        $arr_pice = range(1, $this->mapping['properties']['param']['indexSegNum']);
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/terms/';
        $zm = array(
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'
        );














        foreach ($zm as $z) {
            $PostingListArr = [];
            if (is_dir($indexSegDir)) {
                $file = $indexSegDir . $z . '.index';
                if (is_file($file)) {
                    $file = fopen($file, "r");
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $arr = explode(' ', $line);
                            $term = $arr[0];
                            $id = $arr[1];
                            if (!isset($container[$term]) && ($id != '')) {
                                $container[$term] = $id;
                            } else {
                                $container[$term] .= ',' . $id;
                            }
                        }
                    }
                    foreach ($container as $query => $v) {
                        $v = trim($v, ',');
                        if (!isset($PostingListArr[$query])) {
                            $PostingListArr[$query] = $v;
                        } else {
                            $PostingListArr[$query] = $PostingListArr[$query] . ',' . $v;
                        }
                        unset($container[$query]);
                    }
                    $resContainer = [];
                    $count = 1;
                    foreach ($PostingListArr as $k => $v) {
                        ++$count;
                        $v_arr = explode(',', $v);
                        $arr_slice = $v_arr;
                        $arr_slice_str = $this->differentialCompression($arr_slice);
                        $id_str = $this->systemCompression($arr_slice_str);
                        $resContainer[] = $k . '|' . $id_str;
                        if ($count % 10000 == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                            $resContainer = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                    $resContainer = [];
                    $container = [];
                }
            }
        }
    }


    private function buildBitmapWholeIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', '');
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_postinglist.index', '');
        $arr_pice = range(1, $this->mapping['properties']['param']['indexSegNum']);
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/terms/';
        $zm = array(
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'
        );
























        foreach ($zm as $z) {
            $bitmapArr = [];
            if (is_dir($indexSegDir)) {
                $file = $indexSegDir . $z . '.index';
                if (is_file($file)) {
                    $file = fopen($file, "r");
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $arr = explode(' ', $line);
                            $term = $arr[0];
                            $id = $arr[1];
                            if (!isset($container[$term]) && ($id != '')) {
                                $container[$term] = $id;
                            } else {
                                $container[$term] .= ',' . $id;
                            }
                        }
                    }
                    foreach ($container as $term => $v) {
                        $v = trim($v, ',');
                        $v = explode(',', $v);

                        foreach ($v as $k => $d) {
                            $quotient = floor($d / 64);
                            $remainder = $d % 64;
                            if (!isset($bitmapArr[$term][$quotient])) {
                                $bitmapArr[$term][$quotient] = 1 << (int)$remainder;
                            } else {

                                $bitmapArr[$term][$quotient] = $bitmapArr[$term][$quotient] | (1 << (int)$remainder);
                            }
                        }
                    }
                    $resContainer = [];
                    $count = 1;
                    foreach ($bitmapArr as $term => $v) {
                        ++$count;
                        ksort($v);
                        $idShang = implode(',', array_keys($v));
                        $idShang = $this->differentialCompression($idShang);
                        $idYu = implode(',', array_values($v));
                        $id_str = $this->systemCompression($idShang . '/' . $idYu);
                        $resContainer[] = $term . '|' . $id_str;
                        if ($count % $this->indexDataWriteBufferSize == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                            $resContainer = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                    $resContainer = [];
                    $resContainer = [];
                    $count = 1;
                    foreach ($container as $k => $v) {
                        ++$count;
                        $v = trim($v, ',');
                        $v_arr = explode(',', $v);
                        $arr_slice = $v_arr;
                        $arr_slice_str = $this->differentialCompression($arr_slice);
                        $id_str = $this->systemCompression($arr_slice_str);
                        $resContainer[] = $k . '|' . $id_str;
                        if ($count % $this->indexDataWriteBufferSize == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_postinglist.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                            $resContainer = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_postinglist.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                    $resContainer = [];
                    $container = [];
                }
            }
        }
    }

    private function buildPostingListIncrementIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/terms/';
        if (!is_dir($incrementIndexSegDir)) {
            mkdir($incrementIndexSegDir, 0777);
        }
        if (is_dir($incrementIndexSegDir)) {
            $file_list = scandir($incrementIndexSegDir);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($incrementIndexSegDir . $v)) {
                    $file = fopen($incrementIndexSegDir . $v, "r");
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $arr = explode(' ', $line);
                            $term = $arr[0];
                            $id = $arr[1];
                            if (!isset($container[$term]) && ($id != '')) {
                                $container[$term] = $id;
                            } else {
                                $container[$term] .= ',' . $id;
                            }
                        }
                    }
                    $temp = [];
                    $count = 0;
                    foreach ($container as $k => $v) {
                        ++$count;
                        $v = trim($v, ',');
                        $temp[] = $k . '|' . $v;
                        if ($count % 10000 == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                            $temp = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                    $container = [];
                }
            }
        }

        $this->mergeIncrementPostingListWhole($field);
    }

    private function mergeIncrementPostingListWhole($field)
    {
        $file = $this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index';
        $newTerm = [];
        if (is_file($file)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
            $pdo = new PDO_sqlite($dir);
            $diff = ['\\', HHF];
            $i = 0;
            $pdo->beginTransaction();
            $file = fopen($file, "r");
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line !== '') {
                    ++$i;
                    $v_arr = explode('|', $line);
                    $term = $v_arr[0];
                    if (in_array($term, $diff)) {
                        continue;
                    }
                    $newTerm[] = $term;
                    $posting_list = trim($v_arr[1]);
                    unset($line);
                    unset($v_arr[1]);
                    if ($posting_list) {
                        $sql = "select * from $field where term='$term';";
                        $resRow = $pdo->getRow($sql);
                        if ($resRow) {
                            $line = $resRow;
                            if (is_array($line) && !empty($line)) {
                                $id = $line['id'];
                                $ids = $line['ids'];
                                $term = $line['term'];
                                $ids = $this->systemDecompression($ids);
                                $ids = $this->differentialDecompression($ids);
                                $ids = $ids . ',' . $posting_list;
                                $ids = $this->differentialCompression($ids);
                                $ids = $this->systemCompression($ids);

                                $sql = "update $field set ids='$ids' where id=$id;";
                                $pdo->exec($sql);
                            }
                        } else {
                            if (mb_strlen($term, 'utf-8') < 21) {
                                $ids = $posting_list;
                                $ids = $this->differentialCompression($ids);
                                $ids = $this->systemCompression($ids);
                                $sql = "insert into $field (term,ids) values('$term','$ids')";
                                $pdo->exec($sql);
                                unset($ids);
                            }
                        }
                    }
                    if ($i % $this->indexDataWriteBufferSize == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
            $pdo->commit();
        }
    }


    private function buildBitmapIncrementIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', '');
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_postinglist.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/terms/';
        if (!is_dir($incrementIndexSegDir)) {
            mkdir($incrementIndexSegDir, 0777);
        }
        $file_list = scandir($incrementIndexSegDir);
        if (count($file_list) == 2) {
            return;
        }
        foreach ($file_list as $v) {
            if ($v == '.' || $v == '..') {
                continue;
            }
            if (is_file($incrementIndexSegDir . $v)) {

                $file = fopen($incrementIndexSegDir . $v, "r");
                $container = [];
                while ($line = fgets($file)) {
                    $line = trim($line);
                    if ($line != '') {
                        $arr = explode(' ', $line);
                        $term = $arr[0];
                        $id = $arr[1];
                        if (!isset($container[$term]) && ($id != '')) {
                            $container[$term] = $id;
                        } else {
                            $container[$term] .= ',' . $id;
                        }
                    }
                }
                $temp = [];
                $count = 0;
                foreach ($container as $term => $v) {
                    ++$count;
                    $v = trim($v, ',');
                    $v_arr = explode(',', $v);
                    $bitmapArr = [];
                    foreach ($v_arr as $k => $d) {
                        $quotient = floor($d / 64);
                        $remainder = $d % 64;
                        if (!isset($bitmapArr[$quotient])) {
                            $bitmapArr[$quotient] = 1 << (int)$remainder;
                        } else {

                            $bitmapArr[$quotient] = $bitmapArr[$quotient] | (1 << (int)$remainder);
                        }
                    }



                    $temp[] = $term . '|' . json_encode($bitmapArr);
                    if ($count % $this->indexDataWriteBufferSize == 0) {
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                        $temp = [];
                    }
                }
                file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);



                $temp = [];
                $count = 0;
                foreach ($container as $k => $v) {
                    ++$count;
                    $v = trim($v, ',');
                    $temp[] = $k . '|' . $v;
                    if ($count % $this->indexDataWriteBufferSize == 0) {
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_postinglist.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                        $temp = [];
                    }
                }
                file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_postinglist.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                $container = [];
            }
        }

        $this->mergeIncrementBitmapWhole($field);
        $this->mergeIncrementPostingListWhole($field . '_postinglist');
    }

    private function mergeIncrementBitmapWhole($field)
    {
        $file = $this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index';
        $newTerm = [];
        if (is_file($file)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';

            $pdo = new PDO_sqlite($dir);



            $diff = ['\\', PHP_EOL];
            $i = 0;
            $pdo->beginTransaction();
            $file = fopen($file, "r");
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line !== '') {
                    ++$i;
                    $v_arr = explode('|', $line);
                    $term = $v_arr[0];
                    if (in_array($term, $diff)) {
                        continue;
                    }
                    $newTerm[] = $term;
                    $bitmap = json_decode(trim($v_arr[1]), true);
                    ksort($bitmap);
                    unset($line);
                    unset($v_arr[1]);
                    if (is_array($bitmap)) {
                        $sql = "select * from $field where term='$term';";
                        $resRow = $pdo->getRow($sql);
                        if ($resRow) {
                            $line = $resRow;
                            if (is_array($line) && !empty($line)) {
                                $id = $line['id'];
                                $ids = $line['ids'];
                                $term = $line['term'];
                                $ids = $this->systemDecompression($ids);
                                unset($line);
                                $bmp = explode('/', $ids);
                                $bmp[0] = $this->differentialDecompression($bmp[0]);
                                $shang = explode(',', $bmp[0]);
                                $intlist = explode(',', $bmp[1]);
                                $bitmapArr = array_combine($shang, $intlist);
                                unset($bmp);
                                unset($shang);
                                unset($intlist);
                                foreach ($bitmap as $s => $bit) {
                                    if (isset($bitmapArr[$s])) {
                                        $bitmapArr[$s] = (int)$bit | (int)$bitmapArr[$s];
                                    } else {
                                        $bitmapArr[$s] = (int)$bit;
                                    }
                                }
                                unset($bitmap);
                                ksort($bitmapArr);
                                $idShang = implode(',', array_keys($bitmapArr));
                                $idShang = $this->differentialCompression($idShang);
                                $idYu = implode(',', array_values($bitmapArr));
                                $id_str = $idShang . '/' . $idYu;
                                $ids = $this->systemCompression($id_str);
                                unset($bitmapArr);

                                $sql = "update $field set ids='$ids' where id=$id;";
                                $pdo->exec($sql);
                                unset($id_str);
                            }
                        } else {
                            if (mb_strlen($term, 'utf-8') < 21) {
                                ksort($bitmap);
                                $idShang = implode(',', array_keys($bitmap));
                                $idShang = $this->differentialCompression($idShang);
                                $idYu = implode(',', array_values($bitmap));
                                $id_str = $idShang . '/' . $idYu;
                                $ids = $this->systemCompression($id_str);

                                $sql = "insert into $field (term,ids) values('$term','$ids')";
                                $pdo->exec($sql);
                                unset($bitmap);
                                unset($id_str);
                            }
                        }
                    }
                    if ($i % 10000 == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
            $pdo->commit();
        }
    }

    private function buildGeoPointIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', '');
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/terms/';
        $file = $indexSegDir . '/geo_point_base32.index';
        if (is_file($file)) {
            $file = fopen($file, "r");
            $container = [];
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line != '') {
                    $arr = explode(' ', $line);
                    $id = $arr[0];
                    $point = $arr[1];
                    $strlen = mb_strlen($point);
                    $maxlen = ($strlen > 6) ? 7 : $strlen;
                    for ($i = 2; $i <= $maxlen; $i++) {
                        $sub = mb_substr($point, 0, $i);
                        if ($sub) {
                            $container[$sub][] = $id;
                        }
                    }
                }
            }
            $temp = [];
            $count = 0;
            foreach ($container as $k => $v) {
                ++$count;
                $postlist = implode(',', $v);
                $ids = $this->differentialCompression($postlist);
                $ids = $this->systemCompression($ids);
                $temp[] = $k . '|' . $ids;
                if ($count % 10000 == 0) {
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                    $temp = [];
                }
            }
            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
        }
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_radian_' . $field . '.index', '');
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/terms/';
        $file = $indexSegDir . 'geo_point_radian.index';
        if (is_file($file)) {
            $file = fopen($file, "r");
            $container = [];
            $count = 0;
            while ($line = fgets($file)) {
                ++$count;
                $line = trim($line);
                if ($line != '') {
                    $arr = explode(' ', $line);
                    $id = $arr[0];
                    $point = $arr[1];
                    $container[] = $id . '|' . $point;
                    if ($count % 10000 == 0) {
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_radian_' . $field . '.index', implode(PHP_EOL, $container) . PHP_EOL, FILE_APPEND);
                        $container = [];
                    }
                }
            }
            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_radian_' . $field . '.index', implode(PHP_EOL, $container) . PHP_EOL, FILE_APPEND);
        }
    }

    private function buildGeoPointIncrementIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/terms/';
        $file = $incrementIndexSegDir . 'geo_point.index';
        if (is_file($file)) {
            $file = fopen($file, "r");
            $container = [];
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line != '') {
                    $arr = explode(' ', $line);
                    $id = $arr[0];
                    $point = $arr[1];
                    $strlen = mb_strlen($point);
                    $maxlen = ($strlen > 6) ? 7 : $strlen;
                    for ($i = 1; $i <= $maxlen; $i++) {
                        $sub = mb_substr($point, 0, $i);
                        if ($sub) {
                            if (!isset($container[$sub])) {
                                $container[$sub] = [];
                                $container[$sub][] = $id;
                            } else {
                                $container[$sub][] = $id;
                            }
                        }
                    }
                }
            }
            $temp = [];
            $count = 0;
            foreach ($container as $k => $v) {
                ++$count;
                $postlist = implode(',', $v);
                $temp[] = $k . '|' . $postlist;
                if ($count % $this->indexDataWriteBufferSize == 0) {
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
                    $temp = [];
                }
            }
            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index', implode(PHP_EOL, $temp) . PHP_EOL, FILE_APPEND);
        }

        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/terms/';
        $file = $incrementIndexSegDir . 'geo_point_radian.index';
        if (is_file($file)) {
            $file = fopen($file, "r");
            $container = [];
            $count = 0;
            while ($line = fgets($file)) {
                ++$count;
                $line = trim($line);
                if ($line != '') {
                    $arr = explode(' ', $line);
                    $id = $arr[0];
                    $point = $arr[1];
                    $container[] = $id . '|' . $point;
                    if ($count % $this->indexDataWriteBufferSize == 0) {
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_radian_' . $field . '.index', implode(PHP_EOL, $container) . PHP_EOL, FILE_APPEND);
                        $container = [];
                    }
                }
            }
            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_radian_' . $field . '.index', implode(PHP_EOL, $container) . PHP_EOL, FILE_APPEND);
        }
        $this->mergeGeoPointIncrementIndexWhole($field);
    }

    private function mergeGeoPointIncrementIndexWhole($field)
    {
        $this->mergeIncrementPostingListWhole($field);
        $file = $this->indexDir . $this->IndexName . '/summarizedData/increment_dp_radian_' . $field . '.index';
        if (is_file($file)) {
            $field_geo = $field . '_radian';
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_geo . '.db';
            $pdo = new PDO_sqlite($dir);
            $count = 0;
            $pdo->beginTransaction();
            $file = fopen($file, "r");
            while ($line = fgets($file)) {
                ++$count;
                $line = trim($line);
                if ($line != '') {
                    $arr = explode('|', $line);
                    $id = $arr[0];
                    $radin = $arr[1];
                    $sql = "insert into $field_geo (term,ids)values('$id','$radin');";
                    $pdo->exec($sql);
                    if ($count % $this->indexDataWriteBufferSize == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
            $pdo->commit();
        }
    }

    private function differentialCompression($postlist)
    {
        if ($this->primarykeyType !== 'Int_Incremental') {
            if (is_array($postlist)) {
                return implode(',', $postlist);
            } else if (is_string($postlist)) {
                return $postlist;
            } else {
                return '';
            }
        }
        if (is_array($postlist)) {
            if (count($postlist) > 0) {
                sort($postlist);
                $previousId = array_shift($postlist);
                $res = [$previousId];
                foreach ($postlist as $d) {
                    $diff = $d - $previousId;
                    $res[] = $diff;
                    $previousId = $d;
                }
                return implode(',', $res);
            } else {
                return '';
            }
        } else if (is_string($postlist)) {
            if (strlen($postlist) > 0) {
                $postlist = explode(',', $postlist);
                sort($postlist);
                $previousId = array_shift($postlist);
                $res = [$previousId];
                foreach ($postlist as $d) {
                    $diff = $d - $previousId;
                    $res[] = $diff;
                    $previousId = $d;
                }
                return implode(',', $res);
            } else {
                return '';
            }
        } else {
            return '';
        }
    }

    private function differentialDecompression($postlist)
    {
        if ($this->primarykeyType !== 'Int_Incremental') {
            return $postlist;
        }
        $postlist = (string)$postlist;
        if (strlen($postlist) > 0) {
            $postlist = explode(',', $postlist);
            $previousId = array_shift($postlist);
            $res = [$previousId];
            foreach ($postlist as $d) {
                $realId = $previousId + $d;
                $res[] = $realId;
                $previousId = $realId;
            }
            return implode(',', $res);
        } else {
            return '';
        }
    }

    private function systemCompression($string)
    {
        return base64_encode(gzdeflate($string));
    }

    private function systemDecompression($string)
    {
        return gzinflate(base64_decode($string));
    }

    private function buildIncrementAutoCompletionIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_auto_completion.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/' . $field . '/';
        if (!is_dir($incrementIndexSegDir)) {
            mkdir($incrementIndexSegDir, 0777);
        }
        if (is_dir($incrementIndexSegDir)) {
            $file_list = scandir($incrementIndexSegDir);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($incrementIndexSegDir . $v)) {
                    $file = fopen($incrementIndexSegDir  . $v, "r");
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $arr = explode(' ', $line);
                            $prefix = $arr[0];
                            $list = json_decode($arr[1], true);
                            if (!isset($container[$prefix])) {
                                $container[$prefix] = $list;
                            } else {
                                $container[$prefix] = array_merge($container[$prefix], $list);
                            }
                        }
                    }
                    foreach ($container as $k => $v) {
                        $container[$k] = array_slice(array_unique($v), 0, $this->autoCompleteMaxNum);
                    }


                    $resContainer = [];
                    $count = 1;
                    foreach ($container as $k => $v) {
                        ++$count;
                        $json = json_encode($v);
                        $resContainer[] = $k . '|' . $json;
                        if ($count % $this->indexDataWriteBufferSize == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_auto_completion.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                            $resContainer = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_auto_completion.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                    $resContainer = [];
                    $container = [];
                }
            }
            $this->mergeIncrementAutoCompletion($field);
        }
    }

    private function mergeIncrementAutoCompletion($field)
    {
        $file = $this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_auto_completion.index';
        $newTerm = [];
        $field_completion = $field . '_completion';
        if (is_file($file)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_completion . '.db';
            $pdo = new PDO_sqlite($dir);
            $diff = ['\\', HHF];
            $i = 0;
            $pdo->beginTransaction();
            $file = fopen($file, "r");
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line !== '') {
                    ++$i;
                    $v_arr = explode('|', $line);
                    $prefix = $v_arr[0];
                    $incrListJson  = $v_arr[1];
                    if (in_array($prefix, $diff)) {
                        continue;
                    }
                    if ($prefix) {
                        $sql = "select * from $field_completion where term='$prefix';";
                        $resRow = $pdo->getRow($sql);
                        if ($resRow) {
                            $id = $resRow['id'];
                            $oldList = $resRow['ids'];
                            $oldList = $this->systemDecompression($oldList);
                            $oldList = (array)json_decode($oldList, true);
                            $incrList = (array)json_decode($incrListJson, true);
                            $newList = array_unique(array_merge($incrList, $oldList));
                            $newList = array_slice($newList, 0, $this->autoCompleteMaxNum);
                            $newList = json_encode($newList);
                            $newList = $this->systemCompression($newList);
                            $sql = "update $field_completion set ids='$newList' where id='$id';";
                            $pdo->exec($sql);
                        } else {
                            if (mb_strlen($prefix, 'utf-8') < 21) {
                                $ids = $this->systemCompression($incrListJson);
                                $sql = "insert into $field_completion (term,ids) values('$prefix','$ids');";
                                $pdo->exec($sql);
                                unset($ids);
                            }
                        }
                    }
                    if ($i % $this->indexDataWriteBufferSize == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
            $pdo->commit();
        }
    }

    private function buildAutoCompletionIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_auto_completion.index', '');
        $AutoCompletionIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIndexSeg/' . $field . '/';
        if (!is_dir($AutoCompletionIndexSegDir)) {
            mkdir($AutoCompletionIndexSegDir, 0777);
        }
        if (is_dir($AutoCompletionIndexSegDir)) {
            $container = [];
            $file_list = scandir($AutoCompletionIndexSegDir);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($AutoCompletionIndexSegDir  . $v)) {
                    $file = fopen($AutoCompletionIndexSegDir  . $v, "r");
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $arr = explode(' ', $line);
                            $prefix = $arr[0];
                            $list = json_decode($arr[1], true);
                            if (!isset($container[$prefix])) {
                                $container[$prefix] = $list;
                            } else {
                                $container[$prefix] = array_merge($container[$prefix], $list);
                            }
                        }
                    }
                    foreach ($container as $k => $v) {
                        $container[$k] = array_slice(array_unique($v), 0, $this->autoCompleteMaxNum);
                    }
                    $resContainer = [];
                    $count = 1;
                    foreach ($container as $k => $v) {
                        ++$count;
                        $v = $this->systemCompression(json_encode($v));
                        $resContainer[] = $k . '|' . $v;
                        if ($count % 10000 == 0) {
                            file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_auto_completion.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                            $resContainer = [];
                        }
                    }
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_auto_completion.index', implode(PHP_EOL, $resContainer) . PHP_EOL, FILE_APPEND);
                    $resContainer = [];
                    $container = [];
                }
            }
        }
    }

    private function buildCheckSensitiveIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_check_sensitive.index', '');
        $CheckSensitiveIndexSegDir = $this->indexDir . $this->IndexName . '/makeCheckSensitiveIndexSeg/' . $field . '/';
        if (!is_dir($CheckSensitiveIndexSegDir)) {
            mkdir($CheckSensitiveIndexSegDir, 0777);
        }
        if (is_dir($CheckSensitiveIndexSegDir)) {
            $file_list = scandir($CheckSensitiveIndexSegDir);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($CheckSensitiveIndexSegDir  . $v)) {
                    $file = fopen($CheckSensitiveIndexSegDir  . $v, "r");
                    $term = substr($v, 0, -6);
                    $container = [];
                    while ($line = fgets($file)) {
                        $line = trim($line);
                        if ($line != '') {
                            $container = array_merge($container, explode(',', $line));
                        }
                    }
                    $container = array_unique($container);
                    $container = $this->systemCompression(json_encode($container));
                    $str = $term . '|' . $container;
                    file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_check_sensitive.index', $str . PHP_EOL, FILE_APPEND);
                    $container = [];
                }
            }
        }
    }
    private function buildNumericIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', '');
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/interval/';
        $listDir = $indexSegDir . 'list.txt';
        if (is_file($listDir)) {
            $file = fopen($listDir, "r");
            $count = [];
            while ($line = fgets($file)) {
                if ($line !== '') {
                    $line = trim($line);
                    list($d, $n) = explode(' ', $line);
                    if (!isset($count['min'])) {
                        $count['min'] = $n;
                    } else {
                        if ($n <= $count['min']) {
                            $count['min'] = $n;
                        }
                    }
                    if (!isset($count['max'])) {
                        $count['max'] = $n;
                    } else {
                        if ($n >= $count['max']) {
                            $count['max'] = $n;
                        }
                    }
                }
            }
            fclose($file);
            $difference = ceil($count['max'] - $count['min']);
            if ($difference <= 200) {
                $range = 1;
                $this->intervalNum = $difference;
                $count['max'] = ceil($count['max']);
            } else {
                $range = $difference / $this->intervalNum;
            }
            $interval_mapping = [];
            for ($i = 0; $i < $this->intervalNum; ++$i) {
                $interval_mapping[$i] = $count['min'] + $i * $range;
            }
            $interval_mapping[$this->intervalNum] = $count['max'];
            file_put_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $field . '_interval_mapping.txt', json_encode($interval_mapping));
            $file = fopen($listDir, "r");
            $temp = [];
            $group = [];
            while ($line = fgets($file)) {
                if ($line != '') {
                    $line = trim($line);
                    list($d, $n) = explode(' ', $line);
                    $interval = $this->getNumericInterval($n, $field);
                    if (!isset($temp[$interval][$n])) {
                        $temp[$interval][$n] = $d;
                        $group[$interval][$n] = $d;
                    } else {
                        $temp[$interval][$n] .= ',' . $d;
                    }
                }
            }
            fclose($file);

            $build = [];
            foreach ($temp as $i => $l) {
                $build[$i] = [
                    $l,                     $group[$i],
                ];
            }
            unset($temp);
            foreach ($build as $interval => $l) {

                $json = json_encode($l);
                $ids = $this->systemCompression($json);
                file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', $interval . '|' . $ids . PHP_EOL, FILE_APPEND);
            }
        }
    }
    private function buildDateIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', '');
        $indexSegDir = $this->indexDir . $this->IndexName . '/makeIndexSeg/' . $field . '/interval/';
        $intervalArr = [];
        if (is_dir($indexSegDir)) {
            $file_list = scandir($indexSegDir);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($indexSegDir . $v)) {
                    $interval = mb_substr($v, 0, -4);
                    $intervalArr[] = $interval;
                }
            }
            $intervalArr = array_map('intval', $intervalArr);
            sort($intervalArr);
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($indexSegDir . $v)) {
                    $file = fopen($indexSegDir . $v, "r");
                    $postlist = '';
                    while ($line = fgets($file)) {
                        if ($line != '') {
                            $line = trim($line);
                            $postlist .=  $line . ',';
                        }
                    }
                    $postlist = mb_substr($postlist, 0, -1);
                    $interval = mb_substr($v, 0, -4);
                    $interval = $this->getDateNYR(strtotime($interval));
                    if ($interval !== false) {
                        $ids = $this->differentialCompression($postlist);
                        $ids = $this->systemCompression($ids);
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', $interval . '|' . $ids . PHP_EOL, FILE_APPEND);
                    }
                    $postlist = '';
                }
            }
        }
        $file = $this->indexDir . $this->IndexName . '/index/interval_mapping/' . $field . '_interval_mapping.txt';
        file_put_contents($file, json_encode($intervalArr));
    }
    private function buildTermsPrefixIndex()
    {

        $dir = $this->indexDir . $this->IndexName . '/index/_prefix_index.db';
        if (is_file($dir)) {
            unlink($dir);
        }
        if (is_file($dir . '-journal')) {
            unlink($dir . '-journal');
        }
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            $type = isset($fd['type']) ? $fd['type'] : '';
            if ($type !== 'geo_point') {
                $this->buildIndexTermsPrefixMapping($field);
            }
        }
    }
    private function buildTermsSuffixIndex()
    {
        $dir = $this->indexDir . $this->IndexName . '/index/_suffix_index.db';
        if (is_file($dir)) {
            unlink($dir);
        }
        if (is_file($dir . '-journal')) {
            unlink($dir . '-journal');
        }
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            $type = isset($fd['type']) ? $fd['type'] : '';
            if ($type !== 'geo_point') {
                $this->buildIndexTermsSuffixMapping($field);
            }
        }
    }

    public function buildIndex()
    {
        foreach ($this->mapping['properties']['allFieldType']['numeric'] as $k => $field) {
            $this->buildNumericIndex($field);
            $field_interval = $field . '_interval';
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
            if (is_file($dir)) {
                unlink($dir);
            }
            if (is_file($dir . '-journal')) {
                unlink($dir . '-journal');
            }
            $pdo = new PDO_sqlite($dir);

            $sql_table = "CREATE TABLE IF NOT EXISTS $field_interval (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
            $pdo->exec($sql_table);
            $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field_interval . "_term ON " . $field_interval . "(term);";
            $pdo->exec($sql_index);
            $storageDir = $this->getStorageDir();
            $specArr = ['\\', PHP_EOL];
            $file = fopen($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', "r");
            $i = 0;
            $pdo->beginTransaction();
            $rows = $this->yield_fread_row();
            foreach ($rows($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index') as $line) {
                $line = trim($line);
                if ($line !== '') {
                    ++$i;
                    list($q, $d) = explode('|', $line);
                    $d = trim($d);
                    if ($q !== '') {
                        if (in_array($q, $specArr)) {
                            continue;
                        }
                        $sql = "insert into $field_interval (term,ids)values('$q','$d')";
                        $pdo->exec($sql);
                        if (($i % 10000) == 0) {
                            $pdo->commit();
                            $pdo->beginTransaction();
                        }
                    }
                }
            }
            $pdo->commit();
        }
        foreach ($this->mapping['properties']['allFieldType']['date'] as $k => $field) {
            $this->buildDateIndex($field);
            $field_interval = $field . '_interval';
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
            if (is_file($dir)) {
                unlink($dir);
            }
            if (is_file($dir . '-journal')) {
                unlink($dir . '-journal');
            }
            $pdo = new PDO_sqlite($dir);

            $sql_table = "CREATE TABLE IF NOT EXISTS $field_interval (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
            $pdo->exec($sql_table);
            $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field_interval . "_term ON " . $field_interval . "(term);";
            $pdo->exec($sql_index);
            $storageDir = $this->getStorageDir();
            $specArr = ['\\', PHP_EOL];
            $file = fopen($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index', "r");
            $i = 0;
            $pdo->beginTransaction();
            $rows = $this->yield_fread_row();
            foreach ($rows($this->indexDir . $this->IndexName . '/summarizedData/dp_' . $field . '_interval.index') as $line) {
                $line = trim($line);
                if ($line !== '') {
                    ++$i;
                    list($q, $d) = explode('|', $line);
                    $d = trim($d);
                    if ($q !== '') {
                        if (in_array($q, $specArr)) {
                            continue;
                        }
                        $sql = "insert into $field_interval (term,ids)values('$q','$d')";
                        $pdo->exec($sql);
                        if (($i % 1000) == 0) {
                            $pdo->commit();
                            $pdo->beginTransaction();
                        }
                    }
                }
            }
            $pdo->commit();
        }
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            $type = isset($fd['type']) ? $fd['type'] : '';
            if ($type == 'geo_point') {
                $this->buildGeoPointIndex($field);
            } else {
                if ($this->primarykeyType == 'Int_Incremental') {
                    $isKeyWordField = $this->isKeyWordField($field);
                    if ($isKeyWordField) {
                        $this->buildPostingListWholeIndex($field);
                    } else {
                        $this->buildBitmapWholeIndex($field);
                    }
                } else {
                    $this->buildPostingListWholeIndex($field);
                }
            }







            if ($type === 'text' || $type === 'keyword' || $type === 'geo_point') {
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
                if (is_file($dir)) {
                    unlink($dir);
                }
                if (is_file($dir . '-journal')) {
                    unlink($dir . '-journal');
                }
                $pdo = new PDO_sqlite($dir);

                $sql_table = "CREATE TABLE IF NOT EXISTS $field (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
                $pdo->exec($sql_table);
                $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field . "_term ON " . $field . "(term);";
                $pdo->exec($sql_index);
            }
            if ($type === 'text') {
                $field_postlist = $field . '_postinglist';
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field_postlist . '.db';
                if (is_file($dir)) {
                    unlink($dir);
                }
                if (is_file($dir . '-journal')) {
                    unlink($dir . '-journal');
                }
                $pdo_bitmap_postlist = new PDO_sqlite($dir);

                $sql_table = "CREATE TABLE IF NOT EXISTS $field_postlist (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
                $pdo_bitmap_postlist->exec($sql_table);
                $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field_postlist . "_term ON " . $field_postlist . "(term);";
                $pdo_bitmap_postlist->exec($sql_index);
            }
            if ($type == 'geo_point') {
                $field_geo = $field . '_radian';
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field_geo . '.db';
                if (is_file($dir)) {
                    unlink($dir);
                }
                if (is_file($dir . '-journal')) {
                    unlink($dir . '-journal');
                }
                $pdo_radian = new PDO_sqlite($dir);
                $sql_table = "CREATE TABLE IF NOT EXISTS $field_geo (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
                $pdo_radian->exec($sql_table);
                $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field_geo . "_term ON " . $field . "(term);";
                $pdo_radian->exec($sql_index);
            }
            if ($type === 'text' || $type === 'keyword' || $type === 'geo_point') {
                $storageDir = $this->getStorageDir();
                $specArr = ['\\', PHP_EOL];
                $file = fopen($storageDir .  'summarizedData/dp_' . $field . '.index', "r");
                $i = 0;
                $pdo->beginTransaction();
                $rows = $this->yield_fread_row();
                foreach ($rows($storageDir .  'summarizedData/dp_' . $field . '.index') as $line) {
                    $line = trim($line);
                    if ($line != '') {
                        ++$i;
                        list($q, $d) = explode('|', $line);
                        $d = trim($d);
                        if ($q !== '') {
                            if (in_array($q, $specArr)) {
                                continue;
                            }
                            $sql = "insert into $field (term,ids)values('$q','$d')";
                            $pdo->exec($sql);
                            if (($i % 100000) == 0) {
                                $pdo->commit();
                                $pdo->beginTransaction();
                            }
                        }
                    }
                }
                $pdo->commit();
            }
            if ($type === 'text') {
                $storageDir = $this->getStorageDir();
                $specArr = ['\\', PHP_EOL];
                $field_postlist = $field . '_postinglist';
                $file = fopen($storageDir .  'summarizedData/dp_' . $field_postlist . '.index', "r");
                $i = 0;
                $pdo_bitmap_postlist->beginTransaction();
                $rows = $this->yield_fread_row();
                foreach ($rows($storageDir .  'summarizedData/dp_' . $field_postlist . '.index') as $line) {
                    $line = trim($line);
                    if ($line != '') {
                        ++$i;
                        list($q, $d) = explode('|', $line);
                        $d = trim($d);
                        if ($q !== '') {
                            if (in_array($q, $specArr)) {
                                continue;
                            }
                            $sql = "insert into $field_postlist (term,ids)values('$q','$d')";
                            $pdo_bitmap_postlist->exec($sql);
                            if (($i % 100000) == 0) {
                                $pdo_bitmap_postlist->commit();
                                $pdo_bitmap_postlist->beginTransaction();
                            }
                        }
                    }
                }
                $pdo_bitmap_postlist->commit();
            }
            if ($type == 'geo_point') {
                $storageDir = $this->getStorageDir();
                $specArr = ['\\', PHP_EOL];
                $file = fopen($storageDir .  'summarizedData/dp_radian_' . $field . '.index', "r");
                $i = 0;
                $pdo_radian->beginTransaction();
                $rows = $this->yield_fread_row();
                foreach ($rows($storageDir .  'summarizedData/dp_radian_' . $field . '.index') as $line) {
                    $line = trim($line);
                    if ($line != '') {
                        ++$i;
                        list($q, $d) = explode('|', $line);
                        $d = trim($d);
                        if ($q !== '') {
                            if (in_array($q, $specArr)) {
                                continue;
                            }
                            $sql = "insert into $field_geo (term,ids)values('$q','$d')";
                            $pdo_radian->exec($sql);
                            if ($i % 100000 == 0) {
                                $pdo_radian->commit();
                                $pdo_radian->beginTransaction();
                            }
                        }
                    }
                }
                $pdo_radian->commit();
            }
        }
        if (!empty($this->mapping['properties']['auto_completion_field'])) {
            foreach ($this->mapping['properties']['auto_completion_field'] as $field) {
                $this->buildAutoCompletionIndex($field);
                $field_autoCompletion = $field . '_completion';
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field_autoCompletion . '.db';
                if (is_file($dir)) {
                    unlink($dir);
                }
                if (is_file($dir . '-journal')) {
                    unlink($dir . '-journal');
                }
                $pdo_autoCompletion = new PDO_sqlite($dir);
                $sql_table = "CREATE TABLE IF NOT EXISTS $field_autoCompletion (
				id INTEGER PRIMARY KEY,
				term TEXT,
				ids TEXT)";
                $pdo_autoCompletion->exec($sql_table);
                $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field_autoCompletion . "_term ON " . $field_autoCompletion . "(term);";
                $pdo_autoCompletion->exec($sql_index);

                $storageDir = $this->getStorageDir();
                $specArr = ['\\', PHP_EOL];
                $file = fopen($storageDir .  'summarizedData/dp_' . $field . '_auto_completion.index', "r");
                $field_autoCompletion = $field . '_completion';
                $i = 0;
                $pdo_autoCompletion->beginTransaction();
                $rows = $this->yield_fread_row();
                foreach ($rows($storageDir .  'summarizedData/dp_' . $field . '_auto_completion.index') as $line) {
                    $line = trim($line);
                    if ($line !== '') {
                        ++$i;
                        list($q, $d) = explode('|', $line);
                        $d = trim($d);
                        if ($q !== '') {
                            if (in_array($q, $specArr)) {
                                continue;
                            }
                            $sql = "insert into $field_autoCompletion (term,ids)values('$q','$d')";
                            $pdo_autoCompletion->exec($sql);
                            if ($i % 100000 == 0) {
                                $pdo_autoCompletion->commit();
                                $pdo_autoCompletion->beginTransaction();
                            }
                        }
                    }
                }
                $pdo_autoCompletion->commit();
            }
        }











        $this->buildTermsPrefixIndex();
        $this->buildTermsSuffixIndex();
        $lines = (int)file_get_contents($this->indexDir . $this->IndexName . '/summarizedData/countIndex');
        $info = array(
            'docNum' => $lines,
            'create_time' => time(),
        );
        file_put_contents($this->indexDir . $this->IndexName . '/index_info', json_encode($info));

        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        $this->empty_dir($incrementIndexSegDir);
        $autoCompletionIncrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/';
        $this->empty_dir($autoCompletionIncrementIndexSegDir);
        $unIndexCount = $this->indexDir . $this->IndexName . '/summarizedData/unIndexCount';
        file_put_contents($unIndexCount, 0);
        $this->delCache();
        $this->delRealTimeIndex();
        $this->createDir();
    }

    private function buildIndexTermsPrefixMapping($field)
    {
        $container = [];
        $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
        if (is_file($dir)) {
            $pdo = new PDO_sqlite($dir);
            $maxMinId = $this->get_minid_maxid_field($pdo, $field);
            $minId = $maxMinId[0];
            $maxId = $maxMinId[1];
            $diff = $maxMinId[2];
            if ($maxId == 0) {
                return [];
            }
            for ($i = $minId; $i < $maxId; ++$i) {
                $sql = "select id,term from $field where id='$i';";
                $resRow = $pdo->getRow($sql);
                if ($resRow) {
                    $term = $resRow['term'];
                    $id = $resRow['id'];
                    $sub1 = mb_substr($term, 0, 1);
                    $sub2 = mb_substr($term, 0, 2);
                    $sub3 = mb_substr($term, 0, 3);

                    $container[$sub1][] = $term;
                    $container[$sub2][] = $term;
                    $container[$sub3][] = $term;
                } else {
                    continue;
                }
            }
        }
        if (!empty($container)) {




            $dir = $this->indexDir . $this->IndexName . '/index/_prefix_index.db';
            $pdo_prefix_index = new PDO_sqlite($dir);
            $sql_table = "CREATE TABLE IF NOT EXISTS $field (
					id INTEGER PRIMARY KEY,
					term TEXT,
					ids TEXT)";
            $pdo_prefix_index->exec($sql_table);
            $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field . "_term ON " . $field . "(term);";
            $pdo_prefix_index->exec($sql_index);
            $pdo_prefix_index->beginTransaction();
            foreach ($container as $k => $v) {
                $list = json_encode(array_unique($v));
                $sql = "insert into $field (term,ids)values('$k','$list')";
                $pdo_prefix_index->exec($sql);
                if (($i % 100000) == 0) {
                    $pdo_prefix_index->commit();
                    $pdo_prefix_index->beginTransaction();
                }
            }
            $pdo_prefix_index->commit();
        }
    }

    private function buildIndexTermsSuffixMapping($field)
    {
        $container = [];
        $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
        if (is_file($dir)) {
            $pdo = new PDO_sqlite($dir);
            $maxMinId = $this->get_minid_maxid_field($pdo, $field);
            $minId = $maxMinId[0];
            $maxId = $maxMinId[1];
            $diff = $maxMinId[2];
            if ($maxId == 0) {
                return [];
            }
            for ($i = $minId; $i < $maxId; ++$i) {
                $sql = "select id,term from $field where id='$i';";
                $resRow = $pdo->getRow($sql);
                if ($resRow) {
                    $term = $resRow['term'];
                    $id = $resRow['id'];
                    $sub1 = mb_substr($term, -1);
                    $sub2 = mb_substr($term, -2);
                    $sub3 = mb_substr($term, -3);

                    $container[$sub1][] = $term;
                    $container[$sub2][] = $term;
                    $container[$sub3][] = $term;
                } else {
                    continue;
                }
            }
        }
        if (!empty($container)) {
            $dir = $this->indexDir . $this->IndexName . '/index/_suffix_index.db';
            $pdo_suffix_index = new PDO_sqlite($dir);
            $sql_table = "CREATE TABLE IF NOT EXISTS $field (
					id INTEGER PRIMARY KEY,
					term TEXT,
					ids TEXT)";
            $pdo_suffix_index->exec($sql_table);
            $sql_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_" . $field . "_term ON " . $field . "(term);";
            $pdo_suffix_index->exec($sql_index);
            $pdo_suffix_index->beginTransaction();
            foreach ($container as $k => $v) {
                $list = json_encode(array_unique($v));
                $sql = "insert into $field (term,ids)values('$k','$list')";
                $pdo_suffix_index->exec($sql);
                if (($i % 100000) == 0) {
                    $pdo_suffix_index->commit();
                    $pdo_suffix_index->beginTransaction();
                }
            }
            $pdo_suffix_index->commit();
        }
    }
    private $isIncrIndex = false;

    public function onIncrIndex()
    {
        $this->isIncrIndex = true;
    }
    public function closeIncrIndex()
    {
        $this->isIncrIndex = false;
    }

    private function buildNumericIncrementIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_interval.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/interval/';
        $listDir = $incrementIndexSegDir . 'list.txt';
        if (is_file($listDir)) {
            $file = fopen($listDir, "r");
            $temp = [];
            $group = [];
            while ($line = fgets($file)) {
                if ($line != '') {
                    $line = trim($line);
                    list($d, $n) = explode(' ', $line);
                    $interval = $this->getNumericInterval($n, $field);
                    if (!isset($temp[$interval][$n])) {
                        $temp[$interval][$n] = $d;
                        $group[$interval][$n] = $d;
                    } else {
                        $temp[$interval][$n] .= ',' . $d;
                    }
                }
            }
            fclose($file);

            $build = [];
            foreach ($temp as $i => $l) {
                $build[$i] = [
                    $l,                     $group[$i],
                ];
            }
            unset($temp);
            foreach ($build as $i => $l) {
                $interval = $i;
                $json = json_encode($l);
                file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_interval.index', $interval . '|' . $json . PHP_EOL, FILE_APPEND);
            }
            $field = $field . '_interval';
            $this->mergeIncrementNumeric($field);
        }
    }
    private function mergeIncrementNumeric($field)
    {
        $file = $this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '.index';
        $newTerm = [];
        if (is_file($file)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
            $pdo = new PDO_sqlite($dir);
            $diff = ['\\', HHF];
            $i = 0;
            $pdo->beginTransaction();
            $file = fopen($file, "r");
            while ($line = fgets($file)) {
                $line = trim($line);
                if ($line != '') {
                    ++$i;
                    $v_arr = explode('|', $line);
                    $term = $v_arr[0];
                    if (in_array($term, $diff)) {
                        continue;
                    }
                    $newTerm[] = $term;
                    $posting_list = json_decode($v_arr[1], true);
                    unset($line);
                    unset($v_arr[1]);
                    if ($posting_list) {
                        $sql = "select * from $field where term='$term';";
                        $resRow = $pdo->getRow($sql);
                        if ($resRow) {
                            $line = $resRow;
                            if (is_array($line) && !empty($line)) {
                                $id = $line['id'];
                                $ids = $line['ids'];
                                $term = $line['term'];
                                $ids = $this->systemDecompression($ids);
                                $ids = json_decode($ids, true);
                                if (!empty($posting_list)) {
                                    foreach ($posting_list[0] as $num => $str) {
                                        if (isset($ids[0][$num])) {
                                            $ids[0][$num] .= ',' . $str;
                                        } else {
                                            $ids[0][$num] = $str;
                                            $ids[1][$num] = $posting_list[1][$num];
                                        }
                                    }
                                }
                                $ids = json_encode($ids);
                                $ids = $this->systemCompression($ids);

                                $sql = "update $field set ids='$ids' where id=$id;";
                                $pdo->exec($sql);
                            }
                        } else {
                            if (mb_strlen($term, 'utf-8') < 21) {
                                $ids = $posting_list;
                                $ids = $this->systemCompression($ids);
                                $sql = "insert into $field (term,ids) values('$term','$ids')";
                                $pdo->exec($sql);
                                unset($ids);
                            }
                        }
                    }
                    if ($i % $this->indexDataWriteBufferSize == 0) {
                        $pdo->commit();
                        $pdo->beginTransaction();
                    }
                }
            }
            $pdo->commit();
        }
    }

    private function buildDateIncrementIndex($field)
    {
        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_interval.index', '');
        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/' . $field . '/interval/';
        $baseInterval = [];
        if (is_dir($incrementIndexSegDir)) {
            $file_list = scandir($incrementIndexSegDir);

            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($incrementIndexSegDir . $v)) {
                    $interval = mb_substr($v, 0, -4);
                    $interval = $this->getDateNYR(strtotime($interval));
                    $baseInterval[] = $interval;
                }
            }
            $interval_mapping = (array)json_decode(file_get_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $field . '_interval_mapping.txt'), true);
            $interval_mapping = array_unique(array_merge($interval_mapping, $baseInterval));
            sort($interval_mapping);
            file_put_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $field . '_interval_mapping.txt', json_encode($interval_mapping));
            foreach ($file_list as $v) {
                if ($v == '.' || $v == '..') {
                    continue;
                }
                if (is_file($incrementIndexSegDir . $v)) {
                    $file = fopen($incrementIndexSegDir . $v, "r");
                    $postlist = '';
                    while ($line = fgets($file)) {
                        if ($line != '') {
                            $line = trim($line);
                            $postlist .=  $line . ',';
                        }
                    }
                    $postlist = mb_substr($postlist, 0, -1);
                    $interval = mb_substr($v, 0, -4);
                    $interval = $this->getDateNYR(strtotime($interval));
                    if ($interval !== false) {
                        file_put_contents($this->indexDir . $this->IndexName . '/summarizedData/increment_dp_' . $field . '_interval.index', $interval . '|' . $postlist . PHP_EOL, FILE_APPEND);
                    }
                    $postlist = '';
                }
            }

            $field = $field . '_interval';
            $this->mergeIncrementPostingListWhole($field);
        }
    }

    public function buildIncrementIndex()
    {
        foreach ($this->mapping['properties']['allFieldType']['numeric'] as $k => $field) {
            $this->buildNumericIncrementIndex($field);
        }
        foreach ($this->mapping['properties']['allFieldType']['date'] as $k => $field) {
            $this->buildDateIncrementIndex($field);
        }
        foreach ($this->mapping['properties']['field'] as $fd) {
            $field = $fd['name'];
            $type = isset($fd['type']) ? $fd['type'] : '';
            if ($type == 'geo_point') {
                $this->buildGeoPointIncrementIndex($field);
            } else {
                if ($this->primarykeyType == 'Int_Incremental') {
                    $isKeyWordField = $this->isKeyWordField($field);
                    if ($isKeyWordField) {
                        $this->buildPostingListIncrementIndex($field);
                    } else {
                        $this->buildBitmapIncrementIndex($field);
                    }
                } else {
                    $this->buildPostingListIncrementIndex($field);
                }
            }
        }
        if (!empty($this->mapping['properties']['auto_completion_field'])) {
            foreach ($this->mapping['properties']['auto_completion_field'] as $field) {
                $this->buildIncrementAutoCompletionIndex($field);
            }
        }
        $this->buildTermsPrefixIndex();
        $this->buildTermsSuffixIndex();

        $lines = (int)file_get_contents($this->indexDir . $this->IndexName . '/summarizedData/countIndex');
        $info = array(
            'docNum' => $lines,
            'create_time' => time(),
        );
        file_put_contents($this->indexDir . $this->IndexName . '/index_info', json_encode($info));

        $incrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeIncrementIndexSeg/';
        $this->empty_dir($incrementIndexSegDir);
        $autoCompletionIncrementIndexSegDir = $this->indexDir . $this->IndexName . '/makeAutoCompletionIncrementIndexSeg/';
        $this->empty_dir($autoCompletionIncrementIndexSegDir);
        $unIndexCount = $this->indexDir . $this->IndexName . '/summarizedData/unIndexCount';
        file_put_contents($unIndexCount, 0);
        $this->delCache();
        $this->delRealTimeIndex();
        $this->createDir();
    }

    public function delIndex()
    {
        $indexName = $this->indexDir . $this->IndexName;
        if (!is_dir($indexName)) {
            return;
        }
        if (is_file($this->indexDir . $this->IndexName . '/summarizedData/countIndex')) {
            unlink($this->indexDir . $this->IndexName . '/summarizedData/countIndex');
        }
        $this->empty_dir($indexName);
        rmdir($indexName);
    }

    public function setPrimarykeyTypeIntIncremental()
    {
        $this->primarykeyType = 'Int_Incremental';
    }

    public function setPrimarykeyTypeUUID()
    {
        $this->primarykeyType = 'UUID';
    }
    private $cacheTimeOut = 0;

    public function onCache($timeOut = 0)
    {
        $this->isCache = true;
        $this->cacheTimeOut = $timeOut;
    }

    public function closeCache()
    {
        $this->isCache = false;
    }

    private function initCache()
    {

        $currDir = $this->getCurrDir();



        $dir = $currDir . 'cache/cache.db';
        $toDir = $currDir . 'cache/';
        if (!is_dir($toDir)) {
            mkdir($toDir, 0777);
        }

        $pdo = new PDO_sqlite($dir);
        $pdo->exec("CREATE TABLE IF NOT EXISTS cache (
			id INTEGER PRIMARY KEY,
			search_key TEXT,
			content TEXT)");
        $sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_cache ON cache(search_key);";
        $pdo->exec($sql);
    }

    private function cache($key, $values)
    {
        if (!$this->isCache) {
            return false;
        }
        $obj = new Cache();
        $obj->setCache($key, $values, $this->cacheTimeOut, $this->IndexName, $this->redis_obj);
        return;
    }

    private function getCache($key)
    {
        if (!$this->isCache) {
            return false;
        }
        $obj = new Cache();
        $res = $obj->getCache($key, $this->IndexName, $this->redis_obj);
        return $res;
    }

    public function delCache()
    {
        $obj = new Cache();
        $obj->delCache();
        return;
        $currDir = $this->getCurrDir();
        $toDir = $currDir . 'cache/';
        $this->del_dir($toDir);
    }


    private $redis_obj = false;
    public function set_redis_object($obj = false)
    {
        if (is_object($obj)) {
            $this->redis_obj = $obj;
        } else {
            return false;
        }
    }


    private function buildTermStepScore($mapping)
    {
        $stopWords = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/stopword/stopword_big.txt'));
        $stopWords = array_flip($stopWords);
        $res = [];
        foreach ($mapping as $t => $ids) {
            if (empty($ids)) {
                continue;
            }
            $ids_len = count($ids);
            if (isset($stopWords[$t])) {
                $baseScore = 3;
            } else {
                $baseScore = 4;
            }
            $temp = [];
            $pad = array_pad($temp, $ids_len, $baseScore);
            $combine = array_combine($ids, $pad);
            $res[] = $combine;
        }
        return $res;
    }

    private function bitmapInverse($k, $bitmapNum)
    {
        $bitmapIndex = array(
            1 => 0,
            2 => 1,
            4 => 2,
            8 => 3,
            16 => 4,
            32 => 5,
            64 => 6,
            128 => 7,
            256 => 8,
            512 => 9,
            1024 => 10,
            2048 => 11,
            4096 => 12,
            8192 => 13,
            16384 => 14,
            32768 => 15,
            65536 => 16,
            131072 => 17,
            262144 => 18,
            524288 => 19,
            1048576 => 20,
            2097152 => 21,
            4194304 => 22,
            8388608 => 23,
            16777216 => 24,
            33554432 => 25,
            67108864 => 26,
            134217728 => 27,
            268435456 => 28,
            536870912 => 29,
            1073741824 => 30,
            2147483648 => 31,
            4294967296 => 32,
            8589934592 => 33,
            17179869184 => 34,
            34359738368 => 35,
            68719476736 => 36,
            137438953472 => 37,
            274877906944 => 38,
            549755813888 => 39,
            1099511627776 => 40,
            2199023255552 => 41,
            4398046511104 => 42,
            8796093022208 => 43,
            17592186044416 => 44,
            35184372088832 => 45,
            70368744177664 => 46,
            140737488355328 => 47,
            281474976710656 => 48,
            562949953421312 => 49,
            1125899906842624 => 50,
            2251799813685248 => 51,
            4503599627370496 => 52,
            9007199254740992 => 53,
            18014398509481984 => 54,
            36028797018963968 => 55,
            72057594037927936 => 56,
            144115188075855872 => 57,
            288230376151711744 => 58,
            576460752303423488 => 59,
            1152921504606846976 => 60,
            2305843009213693952 => 61,
            4611686018427387904 => 62,
            -9223372036854775808 => 63,
        );
        if (isset($this->onlyIntersectCache['_' . $bitmapNum])) {
            $idArr = [];
            foreach ($this->onlyIntersectCache['_' . $bitmapNum] as $v) {
                $idArr[] = $k * 64 + $v;
            }
            return $idArr;
        } else {
            $idArr = [];
            $onlyIntersect = [];
            if (isset($bitmapIndex[$bitmapNum])) {
                $idArr[] = $k * 64 + $bitmapIndex[$bitmapNum];
                $onlyIntersect[] = $bitmapIndex[$bitmapNum];
            } else {
                foreach ($bitmapIndex as $dec => $v) {
                    if ($dec & $bitmapNum) {
                        $idArr[] = $k * 64 + $v;
                        $onlyIntersect[] = $v;
                    }
                }
            }

            if (isset($onlyIntersect[0])) {
                $this->onlyIntersectCache['_' . $bitmapNum] = $onlyIntersect;
            }
            return $idArr;
        }
    }
    public $termAvailableNum = 0;
    public $bitmapArrMerge = [];
    public $type = 1;
    public $operator;
    public $minimum_should_match = 0;
    public $fieldNum;
    private function core()
    {

        $bitmapArrMergeCount = [];
        $bitmapArrMergeCircleCount = [];
        $bitmapCumulativeMarkFlipTmp = [];
        $bitmapCumulativeMarkFlip = [];
        $conformCount = 0;
        $hit7Term = 0;
        $hit9Term = 0;
        $hit11Term = 0;
        $hit13Term = 0;
        $hit3Term = 0;
        $hit5Term = 0;
        $hit15Term = 0;
        $hit17Term = 0;








        foreach ($this->bitmapArrMerge as $k => $v) {

            $this->btimapIntersectCache = [];
            if (gettype($v) === 'array') {

                $v_len = count($v);


                if ($v_len == 1) {
                    continue;
                }
                if ($this->operator == 'and') {
                    if ($v_len < $this->termAvailableNum) {
                        continue;
                    }
                }
                if ($this->type == 2) {
                    if ($v_len < ($this->termAvailableNum - 2)) {
                        continue;
                    }
                } else if ($this->type == 3) {
                    if ($v_len > ($this->termAvailableNum - 3)) {
                        continue;
                    }
                }
                if ($this->hit3Term) {
                    if ($v_len < 3) {
                        continue;
                    }
                }
                if ($this->hit5Term) {
                    if ($v_len < 6) {
                        continue;
                    }
                }
                if ($this->hit7Term) {
                    if ($v_len < 8) {
                        continue;
                    }
                }
                if ($this->hit9Term) {
                    if ($v_len < 10) {
                        continue;
                    }
                }
                if ($this->hit11Term) {
                    if ($v_len < 12) {
                        continue;
                    }
                }
                if ($this->hit13Term) {
                    if ($v_len < 14) {
                        continue;
                    }
                }
                if ($this->hit15Term) {
                    if (($v_len < 16)) {
                        continue;
                    }
                }
                if ($this->hit17Term) {
                    if (($v_len < $this->termAvailableNum)) {
                        continue;
                    }
                }
                foreach ($v as $int_k => $int_v) {
                    $v[$int_k] = (int)$int_v;
                }







                for ($i = 0; $i < $v_len; ++$i) {
                    $bitmapCumulativeMarkFlipTmp = [];
                    $currList = (int)$v[$i];


                    for ($l = ($i + 1); $l < $v_len; ++$l) {


                        $btimapIntersect = ($currList & $v[$l]);

                        if ($btimapIntersect) {


                            if (isset($this->btimapIntersectCache[$k . '_btimapIntersect_' . $btimapIntersect])) {
                                $decimalArr = $this->btimapIntersectCache[$k . '_btimapIntersect_' . $btimapIntersect];
                            } else {
                                if (isset($this->IntersectCache[$k . '_' . $btimapIntersect])) {
                                    $decimalArr = $this->IntersectCache[$k . '_' . $btimapIntersect];
                                } else {
                                    $decimalArr = $this->bitmapInverse($k, $btimapIntersect);
                                    $this->btimapIntersectCache[$k . '_btimapIntersect_' . $btimapIntersect] = $decimalArr;
                                }
                            }
                            if (!empty($decimalArr)) {

                                foreach ($decimalArr as $e) {

                                    if (isset($bitmapCumulativeMarkFlip[$e])) {
                                        continue;
                                    } else {
                                        if (!isset($bitmapArrMergeCount[$e])) {
                                            $bitmapArrMergeCount[$e] = 2;
                                            $bitmapArrMergeCircleCount[$e] = 2;
                                        } else {
                                            ++$bitmapArrMergeCount[$e];
                                            ++$bitmapArrMergeCircleCount[$e];
                                        }

                                        $bitmapCumulativeMarkFlipTmp[] = $e;
                                    }
                                }
                            }
                        }
                    }


                    foreach ($bitmapCumulativeMarkFlipTmp as $m => $n) {
                        $bitmapCumulativeMarkFlip[$n] = 1;
                    }
                }


                foreach ($bitmapArrMergeCircleCount as $id => $num) {
                    if ($num == $this->termAvailableNum) {
                        ++$conformCount;
                    }

                    if (($this->termAvailableNum > 3) && ($num > 1) && ($num < 4)) {
                        ++$hit3Term;
                    }
                    if (($this->termAvailableNum > 5) && ($num > 3) && ($num < 6)) {
                        ++$hit5Term;
                    }
                    if (($this->termAvailableNum > 7) && ($num > 4) && ($num < 8)) {
                        ++$hit7Term;
                    }
                    if (($this->termAvailableNum > 9) && ($num > 6) && ($num < 10)) {
                        ++$hit9Term;
                    }
                    if (($this->termAvailableNum > 11) && ($num > 7) && ($num < 12)) {
                        ++$hit11Term;
                    }
                    if (($this->termAvailableNum > 13) && ($num > 9) && ($num < 14)) {
                        ++$hit13Term;
                    }
                    if (($this->termAvailableNum > 15) && ($num > 12) && ($num < 16)) {
                        ++$hit15Term;
                    }
                    if (($this->termAvailableNum > 17) && ($num > 14) && ($num < $this->termAvailableNum)) {
                        ++$hit17Term;
                    }
                }

                if ($this->operator == 'and') {
                    if ($conformCount > 1999) {
                        break;
                    }
                } else {
                    if ($this->termAvailableNum < 5) {
                        if ($conformCount > 49) {
                            break;
                        }
                    } else {
                        if ($conformCount > 9) {
                            break;
                        }
                    }
                }
                if ($hit3Term > 200) {
                    $this->hit3Term = true;
                }
                if ($hit5Term > 200) {
                    $this->hit5Term = true;
                }
                if ($hit7Term > 200) {
                    $this->hit7Term = true;
                }
                if ($hit9Term > 200) {
                    $this->hit9Term = true;
                }
                if ($hit11Term > 200) {
                    $this->hit11Term = true;
                }
                if ($hit13Term > 200) {
                    $this->hit13Term = true;
                }
                if ($hit15Term > 200) {
                    $this->hit15Term = true;
                }
                if ($hit17Term > 200) {
                    $this->hit17Term = true;
                }
                $bitmapArrMergeCircleCount = [];
            } else {
                continue;
            }
        }
        return $bitmapArrMergeCount;
    }

    private function bitmapMerge($bitmapArr, $termAvailableNum, $operator, $minimum_should_match = 0)
    {
        $calcInfo = array(
            'calcNum' => 0,             'segWordNum' => count($bitmapArr),             'shangNum' => 0,             'isIntersection' => true,             'maxScore' => 0,
            'total' => 0,             'total_show' => '',
        );
        $bitmapArrMerge = [];
        $bitmapArrMerge = $this->array_merge_recursive(...$bitmapArr);
        $calcInfo['shangNum'] = count($bitmapArrMerge);
        ksort($bitmapArrMerge);

        $this->termAvailableNum = $termAvailableNum;
        $this->bitmapArrMerge = $bitmapArrMerge;
        $this->operator = $operator;
        $this->minimum_should_match = $minimum_should_match;
        $bitmapArrMergeCount = [];
        if ($termAvailableNum > 1) {
            if ($termAvailableNum > 10) {
                $this->type = 2;
                $re1 = $this->core();
                $re2 = [];
                if (empty($re1) || count($re1) < 100) {
                    $this->type = 3;
                    $re2 = $this->core();
                }
                $bitmapArrMergeCount = $re1 + $re2;
            } else {
                $this->type = 1;
                $bitmapArrMergeCount = $this->core();
            }
        }
        if (($this->operator == 'and') && !empty($bitmapArrMergeCount)) {
            $bitmapArrMergeCountTemp = [];
            foreach ($bitmapArrMergeCount as $k => $v) {
                if ($this->termAvailableNum == $v) {
                    $bitmapArrMergeCountTemp[$k] = $v;
                }
            }
            $bitmapArrMergeCount = $bitmapArrMergeCountTemp;
        } else if (($this->operator != 'and')) {
            $lenTerm = $this->termAvailableNum;
            $bitmapArrMergeCount = $this->getDataOfMinimumShouldMatch($this->minimum_should_match, $lenTerm, $bitmapArrMergeCount);
            $comparisonValue = $bitmapArrMergeCount['comparisonValue'];
            $bitmapArrMergeCount = $bitmapArrMergeCount['res'];
        }
        $resultCount = 0;
        if ((count($bitmapArrMergeCount) < 100) && (($this->operator != 'and') || (($this->operator == 'and') && ($this->termAvailableNum < 2))) && ($comparisonValue < 2)) {
            if (empty($bitmapArrMergeCount)) {
                $calcInfo['isIntersection'] = false;
            }
            foreach ($this->bitmapArrMerge as $k => $v) {
                if (is_array($v)) {
                    foreach ($v as $l => $d) {
                        ++$calcInfo['calcNum'];
                        $decimalArr = $this->bitmapInverse($k, $d);
                        foreach ($decimalArr as $e) {
                            if (!isset($bitmapArrMergeCount[$e])) {
                                $bitmapArrMergeCount[$e] = 1;
                                ++$resultCount;
                            } else {
                            }
                        }
                    }
                } else {
                    ++$calcInfo['calcNum'];
                    $decimalArr = $this->bitmapInverse($k, $v);
                    foreach ($decimalArr as $e) {
                        if (!isset($bitmapArrMergeCount[$e])) {
                            $bitmapArrMergeCount[$e] = 1;
                        } else {
                            ++$bitmapArrMergeCount[$e];
                        }
                        ++$resultCount;
                    }
                }

                if ($this->queryList['mode'] !== 'match_nool') {
                    if ($resultCount > $this->searchResMinNum) {
                        break;
                    }
                }
            }
        }
        $calcInfo['maxScore'] = !empty($bitmapArrMergeCount) ? intval(max($bitmapArrMergeCount)) : 0;
        $total = count($bitmapArrMergeCount);
        $calcInfo['total'] = $total;
        if ($total > 1000) {
            $total = substr($total, 0, -3) . '000';
        }
        if ($this->hit5Term || $this->hit7Term  || $this->hit9Term || $this->hit11Term || $this->hit13Term  || $this->hit15Term || $this->hit17Term) {
            $calcInfo['total_show'] = '多于' . $total;
        } else {
            $calcInfo['total_show'] = $total;
        }
        $res = array(
            'intersection' => $bitmapArrMergeCount,
            'info' => $calcInfo,
        );
        return $res;
    }

    private function postingListMatch($queryList, $suffix = '')
    {
        $field = $queryList['field'];
        $select_field = $field . $suffix;
        $operator = $queryList['operator'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $fc_arr = $queryList['fc_arr'];
        $synonym_mapping = $queryList['synonym_mapping'];
        $synonym = $queryList['synonym'];
        $minimum_should_match = $queryList['minimum_should_match'];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未被索引或该字段不存在', 0);
        }
        $resultContainerOfAllTerms = [];
        $count_all = 0;
        $notMerge = $fc_arr;
        $fc_arr = array_unique(array_merge($fc_arr, $synonym));

        $cacheKey = $this->get_cachekey($queryList);

        $resultCache = $this->getCache($cacheKey);

        if ($resultCache) {

            return json_decode($resultCache, true);
        }
        $allArr = [];


        $dir = $this->indexDir . $this->IndexName . '/index/' . $select_field . '.db';

        $pdo = new PDO_sqlite($dir);
        $termsStr = "'" . implode("','", $fc_arr) . "'";
        $sql = "select * from $select_field where term in($termsStr)";
        $resAll = $pdo->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $v) {
                $ids = $v['ids'];
                $term = $v['term'];
                $ids_gzinf = $this->systemDecompression($ids);
                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                $exp = explode(',', $ids_gzinf);
                $allArr[$term] = $exp;
            }
        }
        $id_arr = [];
        foreach ($allArr as $q => $ids) {
            $realTimeData = $this->getRealTimeData($field, $q);
            $exp = explode(',', $realTimeData);
            $ids = array_merge($ids, $exp);
            $resultContainerOfAllTerms[$q] = $ids;
            $id_ = implode(',', $ids);
            unset($exp);
            if ($id_ != '') {
                $id_arr[] = $id_;
            }
        }
        $result = $this->postingListSynonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $id_arr);
        $id_arr = $result['id_arr'];
        $tycResult = $result['tycResult'];



        if (!empty($id_arr)) {
            $id_arr = explode(',', implode(',', $id_arr));
            $id_arr = array_filter($id_arr);
            $deleteList = $this->get_delete_privmarykey();
            $id_arr = array_diff($id_arr, $deleteList);
            if ($this->primarykeyType == 'Int_Incremental') {
                $stepScore = $this->buildTermStepScore($tycResult);
                $arr_score = [];
                if (!empty($stepScore)) {
                    $arr_score = $this->array_merge_recursive(...$stepScore);
                }
                $id_arr_count = array_count_values($id_arr);
                $count_all = count($id_arr_count);
                if (count($notMerge) > 1) {
                    arsort($id_arr_count);
                }
            } else {
                $stepScore = $this->buildTermStepScore($tycResult);
                $arr_score = [];
                if (!empty($stepScore)) {
                    $arr_score = $this->array_merge_recursive(...$stepScore);
                }
                $id_arr_count = array_count_values($id_arr);
                $count_all = count($id_arr_count);

                if (count($notMerge) > 1) {
                    arsort($id_arr_count);
                }
            }
            $filterIds = $this->getFilterIds($queryList['filter'], array_keys($id_arr_count));
            if (is_array($filterIds) && !empty($filterIds)) {
                $filterIdsKeysScore = [];
                foreach ($filterIds as $k => $d) {
                    $filterIdsKeysScore[$d] = $id_arr_count[$d];
                }
                $id_arr_count = $filterIdsKeysScore;
            } else if (is_array($filterIds) && empty($filterIds)) {
                $id_arr_count = [];
            }
            arsort($id_arr_count);

            if ($operator == 'and') {
                $lenTerm = count($notMerge);
                foreach ($id_arr_count as $k => $v) {
                    if ($v < $lenTerm) {
                        unset($id_arr_count[$k]);
                    }
                }
            } else {
                $lenTerm = count($notMerge);
                $id_arr_count = $this->getDataOfMinimumShouldMatch($minimum_should_match, $lenTerm, $id_arr_count);
                $id_arr_count = $id_arr_count['res'];
            }
            $id_slice = array_keys($id_arr_count);
            $arr_new = [];
            foreach ($id_slice as $v) {

                if (is_array($arr_score[$v])) {
                    $arr_new[$v] = array_sum($arr_score[$v]);
                } else {
                    $arr_new[$v] = $arr_score[$v];
                }
            }
            $id_arr = $arr_new;
            arsort($id_arr);
        }

        $id_arr = array_slice($id_arr, 0, $this->searchResMinNum, true);
        $ids_all_score = $id_arr;
        $id_arr = array_keys($id_arr);
        $ids_all = implode(',', $id_arr);
        if (!isset($id_arr[0])) {
            $id_arr = array_values($id_arr);
        }
        $count_all = count($id_arr);
        $id_score = [];
        $curr_page_id_arr = [];
        $qs = ($page - 1) * $listRows;
        $js = ($page - 1) * $listRows + $listRows;
        for ($i = $qs; $i < $js; ++$i) {
            if (!isset($id_arr[$i])) {
                continue;
            }
            array_push($curr_page_id_arr, $id_arr[$i]);
            $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
        }
        if (count($curr_page_id_arr) > 1) {
            $id_str = implode(',', $curr_page_id_arr);
        } else {
            $id_str = isset($curr_page_id_arr[0]) ? $curr_page_id_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'total' => isset($count_all) ? $count_all : 0,             'curr_listRows_real' => count($curr_page_id_arr),
        );
        $info = array(
            'total' => isset($count_all) ? $count_all : 0,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );

        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function match($queryList)
    {
        $field = $queryList['field'];
        $mode = $queryList['mode'];
        $operator = $queryList['operator'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $fc_arr = $queryList['fc_arr'];
        $synonym_mapping = $queryList['synonym_mapping'];
        $synonym = $queryList['synonym'];
        $minimum_should_match = $queryList['minimum_should_match'];
        $id_arr = [];
        $resultContainerOfAllTerms = [];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未被索引或该字段不存在', 0);
        }
        $notMerge = $fc_arr;
        $fc_arr_mer = array_unique(array_merge($fc_arr, $synonym));
        $tempIds = $fc_arr_mer;

        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }

        $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
        $decompression = [];
        $pdo = new PDO_sqlite($dir);
        $tempIds = "'" . implode("','", $tempIds) . "'";
        $sql = "select * from $field where term in($tempIds)";
        $resAll = $pdo->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $v) {
                $ids = $this->systemDecompression($v['ids']);
                $term = $v['term'];
                $decompression[$term] = $ids;
            }
        }






        $bitmapArr = [];
        foreach ($decompression as $q => $shang_ids) {
            if ($shang_ids != '') {
                $line = explode('/', $shang_ids);
                $line[0] = $this->differentialDecompression($line[0]);
                $shang = explode(',', $line[0]);
                $shang = array_map('intval', $shang);
                $ids = explode(',', $line[1]);
                $ids = array_map('intval', $ids);
                $id_ = array_combine($shang, $ids);
            } else {
                continue;
            }
            $szm = $this->getFirstLetter($q);
            $dp_index_block_dir = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
            $index_file = $dp_index_block_dir . '/' . $szm . '/dp.index';
            if (is_file($index_file)) {
                $dp_arr = json_decode(file_get_contents($index_file), true);
                if (is_array($dp_arr) && isset($dp_arr[$q])) {
                    foreach ($dp_arr[$q] as $kz => $vz) {
                        if (!isset($id_[$kz])) {
                            $id_[$kz] = intval($vz);
                        } else {
                            $id_[$kz] = intval($id_[$kz]) | intval($vz);
                        }
                    }
                }
            }
            if ($id_ != '' && !empty($id_)) {
                $bitmapArr[] = $id_;
                $resultContainerOfAllTerms[$q] = $id_;
            }
        }
        $bitmapArr = $this->synonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $bitmapArr);

        unset($resultContainerOfAllTerms);
        unset($bitmapArrMerge);
        unset($bitmapArrMergeSimilar);
        unset($bitmapArrMergeCount);

        $termAvailableNum =  count($notMerge);
        $this->fieldNum = 1;
        $decimalArrAll = $this->bitmapMerge($bitmapArr, $termAvailableNum, $operator, $minimum_should_match);
        $info = $decimalArrAll['info'];
        $decimalArr = $decimalArrAll['intersection'];
        arsort($decimalArr);
        $decimalArrKeys = array_keys($decimalArr);
        $deleteList = $this->get_delete_privmarykey();
        $decimalArrKeys = array_diff($decimalArrKeys, $deleteList);
        $filterIds = $this->getFilterIds($queryList['filter'], $decimalArrKeys);
        if (is_array($filterIds) && !empty($filterIds)) {


            $filterIdsKeysScore = [];
            foreach ($filterIds as $k => $d) {
                $filterIdsKeysScore[$d] = $decimalArr[$d];
            }
            $decimalArr = $filterIdsKeysScore;
        } else if (is_array($filterIds) && empty($filterIds)) {
            $decimalArr = [];
        }
        arsort($decimalArr);
        $total = count($decimalArr);
        $info = [];
        $info['total'] = $total;
        $decimalArr = array_slice($decimalArr, 0, $this->searchResMinNum, true);
        $ids_all_score = $decimalArr;
        $id_arr = array_keys($decimalArr);
        $ids_all = implode(',', $id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );

        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function CalculateIndexDataOfPostlist($query, $suffex = '')
    {
        $resultContainerOfAllTerms = [];
        $fc_arr = $query['fc_arr'];
        $synonym = $query['synonym'];
        $fd = $query['fd'] . $suffex;
        $original_fd = $query['fd'];
        $operator = $query['operator'];
        $synonym_mapping = $query['synonym_mapping'];
        $weight = $query['weight'];
        $minimum_should_match = $query['minimum_should_match'];
        $isKeyWordField = $this->isKeyWordField($fd);
        if ($isKeyWordField) {
            $synonym = [];
            $synonym_mapping = [];
        }
        $notMerge = $fc_arr;
        $fc_arr_mer = array_merge($fc_arr, $synonym);
        $dir = $this->indexDir . $this->IndexName . '/index/' . $fd . '.db';
        $pdo = new PDO_sqlite($dir);
        $allArr = [];
        $tempIds = "'" . implode("','", $fc_arr_mer) . "'";
        $sql = "select * from $fd where term in($tempIds)";
        $resAll = $pdo->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $v) {
                $ids = $v['ids'];
                $term = $v['term'];
                $ids_gzinf = $this->systemDecompression($ids);
                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                $exp = explode(',', $ids_gzinf);
                $allArr[$term] = $exp;
            }
        }
        $id_arr = [];
        foreach ($allArr as $q => $ids) {




            $realTimeData = $this->getRealTimeData($original_fd, $q);
            $exp = explode(',', $realTimeData);
            $ids = array_merge($ids, $exp);
            $resultContainerOfAllTerms[$q] = $ids;
            $id_ = implode(',', $ids);
            unset($exp);
            if ($id_ != '') {
                $id_arr[] = $id_;
            }
        }
        $result = $this->postingListSynonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $id_arr);
        $id_arr = $result['id_arr'];
        $tycResult = $result['tycResult'];
        $result = [];







        if (!empty($id_arr)) {
            $id_arr = explode(',', implode(',', $id_arr));
            $id_arr = array_filter($id_arr);
            $deleteList = $this->get_delete_privmarykey();
            $id_arr = array_diff($id_arr, $deleteList);
            $id_arr_count = array_count_values($id_arr);
            if (count($notMerge) > 1) {

                arsort($id_arr_count);
            }
            if ($operator == 'and') {
                $lenTerm = count($notMerge);
                foreach ($id_arr_count as $k => $v) {
                    if ($v < $lenTerm) {
                        unset($id_arr_count[$k]);
                    }
                }
            } else {
                $lenTerm = count($notMerge);
                $id_arr_count = $this->getDataOfMinimumShouldMatch($minimum_should_match, $lenTerm, $id_arr_count);
                $id_arr_count = $id_arr_count['res'];
            }
            $id_arr_score = $id_arr_count;
            if ($weight > 1) {
                array_walk($id_arr_score, function (&$v, $k, $weight) {
                    $v = $v * $weight;
                }, $weight);
            }
            $result = $id_arr_score;
        }

        return $result;
    }

    private function CalculateIndexDataOfBitmap($query)
    {
        $resultContainerOfAllTerms = [];
        $fc_arr = $query['fc_arr'];
        $synonym = $query['synonym'];
        $fd = $query['fd'];
        $operator = $query['operator'];
        $synonym_mapping = $query['synonym_mapping'];
        $weight = $query['weight'];
        $minimum_should_match = $query['minimum_should_match'];
        $notMerge = $fc_arr;
        $fc_arr_mer = array_unique(array_merge($fc_arr, $synonym));
        $tempIds = $fc_arr_mer;
        $dir = $this->indexDir . $this->IndexName . '/index/' . $fd . '.db';
        $pdo = new PDO_sqlite($dir);
        $tempIds = "'" . implode("','", $tempIds) . "'";
        $sql = "select * from $fd where term in($tempIds)";
        $resAll = $pdo->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $v) {
                $ids = $this->systemDecompression($v['ids']);
                $term = $v['term'];
                $decompression[$term] = $ids;
            }
        }

        $bitmapArr = [];
        foreach ($decompression as $q => $shang_ids) {
            if ($shang_ids != '') {
                $line = explode('/', $shang_ids);
                $line[0] = $this->differentialDecompression($line[0]);
                $shang = explode(',', $line[0]);
                $ids = explode(',', $line[1]);
                $id_ = array_combine($shang, $ids);
            }
            $szm = $this->getFirstLetter($q);
            $dp_index_block_dir = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
            $index_file = $dp_index_block_dir . '/' . $szm . '/dp.index';
            if (is_file($index_file)) {
                $dp_arr = json_decode(file_get_contents($index_file), true);
                if (is_array($dp_arr) && isset($dp_arr[$q])) {
                    foreach ($dp_arr[$q] as $kz => $vz) {
                        if (!isset($id_[$kz])) {
                            $id_[$kz] = intval($vz);
                        } else {
                            $id_[$kz] = intval($id_[$kz]) | intval($vz);
                        }
                    }
                }
            }
            if ($id_ != '' && !empty($id_)) {
                $bitmapArr[] = $id_;
                $resultContainerOfAllTerms[$q] = $id_;
            }
        }
        $bitmapArr = $this->synonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $bitmapArr);

        unset($resultContainerOfAllTerms);
        unset($bitmapArrMerge);
        unset($bitmapArrMergeSimilar);
        unset($bitmapArrMergeCount);

        $termAvailableNum =  count($notMerge);
        $decimalArrAll = $this->bitmapMerge($bitmapArr, $termAvailableNum, $operator, $minimum_should_match);
        $decimalArr = $decimalArrAll['intersection'];
        $decimalArrKeys = array_keys($decimalArr);
        $deleteList = $this->get_delete_privmarykey();
        $decimalArrKeys = array_diff($decimalArrKeys, $deleteList);
        $decimalArrNew = [];
        foreach ($decimalArrKeys as $k => $d) {
            $decimalArrNew[$d] = $decimalArr[$d];
        }
        $decimalArr = $decimalArrNew;
        arsort($decimalArr);
        if ($weight > 1) {
            array_walk($decimalArr, function (&$v, $k, $weight) {
                $v = $v * $weight;
            }, $weight);
        }
        return $decimalArr;
    }

    private function multiMatchEntry($queryList)
    {
        $field = (array)$queryList['field'];
        $field_operator = $queryList['field_operator'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];

        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $fieldNum = count($field);
        $this->fieldNum = $fieldNum;
        $decimalArrList = [];
        foreach ($field as $fdArr) {
            $fd = $fdArr['name'];
            if (!$this->isField($fd)) {
                $this->throwWindException($fd . ' 字段不存在', 0);
            }
            if (!$this->isIndexField($fd)) {
                $this->throwWindException($fd . ' 字段未被索引或该字段不存在', 0);
            }
            $weight = isset($fdArr['weight']) ? ((int)$fdArr['weight'] > 0 ? ceil((int)$fdArr['weight']) : 1) : 1;
            $fc_arr = $fdArr['fc_arr'];
            $synonym_mapping = $fdArr['synonym_mapping'];
            $synonym = $fdArr['synonym'];
            $operator = isset($fdArr['operator']) ?  $fdArr['operator'] : 'or';

            $minimum_should_match = (isset($fdArr['minimum_should_match']) && $fdArr['minimum_should_match'] !== false) ? $fdArr['minimum_should_match'] : 0;
            $query = [
                'fc_arr' => $fc_arr,
                'synonym_mapping' => $synonym_mapping,
                'synonym' => $synonym,
                'weight' => $weight,
                'fd' => $fd,
                'operator' => $operator,
                'minimum_should_match' => $minimum_should_match,
            ];
            if ($this->primarykeyType == 'Int_Incremental') {
                $isKeyWordField = $this->isKeyWordField($fd);
                if ($isKeyWordField) {
                    $decimalArr = $this->CalculateIndexDataOfPostlist($query);
                } else {
                    $query['fd'] = $query['fd'] . '_postinglist';
                    $decimalArr = $this->CalculateIndexDataOfPostlist($query);
                }
            } else {
                $decimalArr = $this->CalculateIndexDataOfPostlist($query);
            }
            $decimalArrList[] = $decimalArr;
        }
        $decimalArrList = !empty($decimalArrList) ? $this->array_merge_recursive(...$decimalArrList) : [];
        foreach ($decimalArrList as $k => $v) {
            if (($field_operator == 'and') && ($fieldNum > 1)) {
                if (!is_array($v)) {
                    unset($decimalArrList[$k]);
                    continue;
                }
                if (count($v) < count($field)) {
                    unset($decimalArrList[$k]);
                    continue;
                }
            }
            if (is_array($v)) {
                $decimalArrList[$k] = array_sum($v);
            }
        }
        $decimalArr = $decimalArrList;
        if (($field_operator == 'or') && ($fieldNum > 1)) {
            arsort($decimalArr);
        } else if (($field_operator == 'and') && ($fieldNum > 1)) {
            arsort($decimalArr);
        } else {
        }
        $filterIds = $this->getFilterIds($queryList['filter'], array_keys($decimalArr));
        if (is_array($filterIds) && !empty($filterIds)) {


            $filterIdsKeysScore = [];
            foreach ($filterIds as $k => $d) {
                $filterIdsKeysScore[$d] = $decimalArr[$d];
            }
            $decimalArr = $filterIdsKeysScore;
        } else if (is_array($filterIds) && empty($filterIds)) {
            $decimalArr = [];
        }
        arsort($decimalArr);
        $decimalArr = array_slice($decimalArr, 0, $this->searchResMinNum, true);
        $ids_all_score = $decimalArr;
        $id_arr = array_keys($decimalArr);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );
        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function boolCoreSearch($queryList)
    {
        $field = (array)$queryList['field'];
        $field_operator = $queryList['operator'];
        $fieldNum = count($field);
        $this->fieldNum = $fieldNum;
        $decimalArrList = [];
        foreach ($field as $fdArr) {
            $fd = $fdArr['name'];
            if (!$this->isField($fd)) {
                $this->throwWindException($fd . ' 字段不存在', 0);
            }
            if (!$this->isIndexField($fd)) {
                $this->throwWindException($fd . ' 字段未被索引或该字段不存在', 0);
            }
            $weight = isset($fdArr['weight']) ? ((int)$fdArr['weight'] > 0 ? ceil((int)$fdArr['weight']) : 1) : 1;
            $fc_arr = $fdArr['fc_arr'];
            $synonym_mapping = $fdArr['synonym_mapping'];
            $synonym = $fdArr['synonym'];
            $operator = $fdArr['operator'];
            $query = [
                'fc_arr' => $fc_arr,
                'synonym_mapping' => $synonym_mapping,
                'synonym' => $synonym,
                'weight' => $weight,
                'fd' => $fd,
                'operator' => $operator,
            ];
            if ($this->primarykeyType == 'Int_Incremental') {
                $isKeyWordField = $this->isKeyWordField($fd);
                if ($isKeyWordField) {
                    $decimalArr = $this->CalculateIndexDataOfPostlist($query);
                } else {
                    $decimalArr = $this->CalculateIndexDataOfPostlist($query, '_postinglist');
                }
            } else {
                $decimalArr = $this->CalculateIndexDataOfPostlist($query);
            }
            $decimalArrList[] = $decimalArr;
        }
        $decimalArrList = !empty($decimalArrList) ? $this->array_merge_recursive(...$decimalArrList) : [];
        foreach ($decimalArrList as $k => $v) {
            if (($field_operator == 'and') && ($fieldNum > 1)) {
                if (!is_array($v)) {
                    unset($decimalArrList[$k]);
                    continue;
                }
                if (count($v) < count($field)) {
                    unset($decimalArrList[$k]);
                    continue;
                }
            }
            if (is_array($v)) {
                $decimalArrList[$k] = array_sum($v);
            }
        }
        $decimalArr = $decimalArrList;
        if (($field_operator == 'or') && ($fieldNum > 1)) {
            arsort($decimalArr);
        } else {
            arsort($decimalArr);
        }
        return $decimalArr;
    }


    private function get_minid_maxid($pdo)
    {

        $sql = "SELECT max($this->sys_primarykey) as max_id FROM $this->IndexName;";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            $maxId = (int)$resRow['max_id'];
        } else {
            $maxId = 0;
        }
        $sql = "SELECT min($this->sys_primarykey) as min_id FROM $this->IndexName;";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            $minId = (int)$resRow['min_id'];
        } else {
            $minId = 0;
        }
        if ($maxId == 0) {
            return [];
        }
        $docNum = $maxId - $minId + 1;
        if ($docNum < 1) {
            return false;
        }
        return [$minId, $maxId, $docNum];
    }

    private function get_minid_maxid_field($pdo, $field = false)
    {
        $sql = "SELECT max(id) as max_id FROM $field;";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            $maxId = (int)$resRow['max_id'];
        } else {
            $maxId = 0;
        }
        $sql = "SELECT min(id) as min_id FROM $field;";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            $minId = (int)$resRow['min_id'];
        } else {
            $minId = 0;
        }
        if ($maxId == 0) {
            return [];
        }
        $docNum = $maxId - $minId + 1;
        if ($docNum < 1) {
            return false;
        }
        return [$minId, $maxId, $docNum];
    }

    private function getRealtimeDate($fd, $indexs = [])
    {
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block/dp_interval.index';
        $res = [];
        if (is_file($dir)) {
            $realtimeIndex = (array)json_decode(file_get_contents($dir), true);
            foreach ($indexs as $i) {
                if (isset($realtimeIndex[$i])) {
                    $res[] = array_merge($realtimeIndex[$i], $res);
                }
            }
        }
        $res = array_merge(...$res);
        return $res;
    }

    private function getRealtimeNumeric($fd, $indexs = [])
    {
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block/dp_interval.index';
        $res = [];
        if (is_file($dir)) {
            $realtimeIndex = (array)json_decode(file_get_contents($dir), true);
            foreach ($indexs as $i) {
                if (isset($realtimeIndex[$i])) {
                    $res[] = $realtimeIndex[$i];
                }
            }
        }
        return $res;
    }
    private $query_point = [];
    private function getNumericNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer = [])
    {
        $intervalIds = [];
        $intervalStr = "'" . implode("','", $intervalContainer) . "'";
        $sql = "select * from $field_interval where term in ($intervalStr);";
        $resAll = $pdo_interval->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $resRow) {
                $ids = $resRow['ids'];
                $ids_gzinf = $this->systemDecompression($ids);
                $intervalIds[] = json_decode($ids_gzinf, true);
            }
            if (!empty($intervalIds)) {
                $list = [];
                foreach ($intervalIds as $k => $v) {
                    $list[] = implode(',', array_values($v[0]));
                }
                $intervalIds = explode(',', implode(',', $list));
            }
        }
        if (!empty($intervalContainer)) {
            $realtime = $this->getRealtimeNumeric($fd, $intervalContainer);
            if (!empty($realtime)) {
                $list = [];
                foreach ($realtime as $k => $v) {
                    $list[] = implode(',', array_values($v[0]));
                }
                $realtime = explode(',', implode(',', $list));
            }
            $intervalIds = array_merge($intervalIds, $realtime);
        }
        return $intervalIds;
    }
    private function getDateNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer = [])
    {
        $intervalIds = [];
        $intervalStr = "'" . implode("','", $intervalContainer) . "'";
        $sql = "select * from $field_interval where term in ($intervalStr);";
        $resAll = $pdo_interval->getAll($sql);
        if ($resAll) {
            foreach ($resAll as $resRow) {
                $ids = $resRow['ids'];
                $ids_gzinf = $this->systemDecompression($ids);
                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                $intervalIds[] = $ids_gzinf;
            }
            if (!empty($intervalIds)) {
                $intervalIds = explode(',', implode(',', $intervalIds));
            }
        }
        if (!empty($intervalContainer)) {
            $realtime = $this->getRealtimeDate($fd, $intervalContainer);
            $intervalIds = array_merge($intervalIds, $realtime);
        }
        return $intervalIds;
    }

    private function getFilterIds($filter, $baseIds = false)
    {
        $conditions = isset($filter['conditions']) ? $filter['conditions'] : [];
        if (empty($conditions)) {
            $conditions = isset($filter['range']) ? $filter['range'] : [];
        }
        if (!is_array($conditions) || empty($conditions)) {
            return $baseIds;
        }
        $foreachIds = $baseIds;
        if (is_array($foreachIds)) {
            if (empty($foreachIds)) {
                return [];
            }
        }
        foreach ($conditions as $field => $cond) {
            $filterConditions = [];
            $fd = $field;
            if (!$this->isField($fd)) {
                $this->throwWindException($fd . ' 字段不存在', 0);
            }
            $fieldType = $this->getFieldType($fd);
            if ($fieldType == false) {
                $this->throwWindException($fd . ' 字段未声明数据类型，无法进行过滤操作', 0);
            }

            $comparison_symbol = ['lt', 'lte', 'gt', 'gte'];
            $condKeys = array_keys($cond);
            foreach ($condKeys as $k => $cd) {
                $terms = $cd;
                $val = $cond[$terms];
                if (in_array($terms, $comparison_symbol)) {
                    $filterConditions[] = [
                        $fd, $terms, $val
                    ];
                    if (count($filterConditions) == 2) {
                        break;
                    }
                }
            }
            $filterConditionsTemp = [];
            if (!empty($filterConditions)) {
                foreach ($filterConditions as $k => $v) {
                    if ($v[1] == 'gt' || $v[1] == 'gte') {
                        $filterConditionsTemp[0] = $v;
                    } else {
                        $filterConditionsTemp[1] = $v;
                    }
                }
                ksort($filterConditionsTemp);
                $filterConditionsTemp = array_values($filterConditionsTemp);
                $filterConditions = $filterConditionsTemp;
            }
            $match_symbols = ['match'];
            if (empty($filterConditions)) {
                foreach ($condKeys as $k => $cd) {
                    $terms = $cd;
                    $val = $cond[$terms];
                    if (in_array($terms, $match_symbols)) {
                        $filterConditions[] = [
                            $fd, $terms, $val
                        ];
                        if (count($filterConditions) == 1) {
                            break;
                        }
                    }
                }
            }
            $eq_symbol = ['eq'];
            if (empty($filterConditions)) {
                foreach ($condKeys as $k => $cd) {
                    $terms = $cd;
                    $val = $cond[$terms];
                    if (in_array($terms, $eq_symbol)) {
                        $filterConditions[] = [
                            $fd, $terms, $val
                        ];
                        if (count($filterConditions) == 1) {
                            break;
                        }
                    }
                }
            }
            $noteq_symbol = ['noteq'];
            if (empty($filterConditions)) {
                foreach ($condKeys as $k => $cd) {
                    $terms = $cd;
                    $val = $cond[$terms];
                    if (in_array($terms, $noteq_symbol)) {
                        $filterConditions[] = [
                            $fd, $terms, $val
                        ];
                        if (count($filterConditions) == 1) {
                            break;
                        }
                    }
                }
            }
            $in_symbol = ['in'];
            if (empty($filterConditions)) {
                foreach ($condKeys as $k => $cd) {
                    $terms = $cd;
                    $val = (array)$cond[$terms];
                    $val = array_map(function ($t) {
                        return $t . '_';
                    }, $val);
                    $val = array_flip($val);
                    if (in_array($terms, $in_symbol)) {
                        $filterConditions[] = [
                            $fd, $terms, $val
                        ];
                        if (count($filterConditions) == 1) {
                            break;
                        }
                    }
                }
            }
            $notin_symbol = ['notin'];
            if (empty($filterConditions)) {
                foreach ($condKeys as $k => $cd) {
                    $terms = $cd;
                    $val = (array)$cond[$terms];
                    $val = array_map(function ($t) {
                        return $t . '_';
                    }, $val);
                    $val = array_flip($val);
                    if (in_array($terms, $notin_symbol)) {
                        $filterConditions[] = [
                            $fd, $terms, $val
                        ];
                        if (count($filterConditions) == 1) {
                            break;
                        }
                    }
                }
            }
            if (empty($filterConditions)) {
                if (isset($cond['distance'])) {
                    $filterConditions['distance'] = $cond['distance'];
                }
                if (isset($cond['geo_point'])) {
                    $filterConditions['geo_point'] = $cond['geo_point'];
                }
            }
            if (empty($filterConditions)) {
                return $baseIds;
            }
            if ($fieldType == 'geo_point') {
                $isIndexField = $this->isIndexField($fd);
                if (!$isIndexField) {
                    $this->throwWindException($fd . ' 字段未配置索引', 0);
                }
                $distance = isset($filterConditions['distance']) ? $filterConditions['distance'] : false;
                $geo_point = isset($filterConditions['geo_point']) ? $filterConditions['geo_point'] : false;
                if (!$distance) {
                    $this->throwWindException('过滤 ' . $fd . ' 字段必须设置距离', 0);
                }
                if (!$geo_point) {
                    $this->throwWindException('过滤 ' . $fd . ' 字段必须设置中心经纬度', 0);
                }
                $query = [
                    'match_geo' => [
                        'field' => [
                            'name' => $fd,                                                                                                                                             'geo_point' => $geo_point,
                            'distance' => $distance,
                        ],
                        'sort' => [
                            'geo_distance' => 'asc'
                        ],
                        'list_rows' => 1000000,                         'page' => 1,                         'baseid' => $foreachIds,
                    ]
                ];
                $res = $this->geoDistanceSearch($query);
                $res = $res['result']['_source'];
                if (!empty($res)) {
                    $primarykeyIds = array_column($res, $this->primarykey);
                    if (is_array($foreachIds)) {
                        $foreachIds = $primarykeyIds;
                    } else {
                        $foreachIds = $primarykeyIds;
                    }
                    if (empty($foreachIds)) {
                        return [];
                    }
                } else {
                    return [];
                }
            } else if (($fieldType === 'numeric')) {
                if (!isset($filterConditions[0][1]) || (!in_array($filterConditions[0][1], $comparison_symbol) && !in_array($filterConditions[0][1], $eq_symbol) && !in_array($filterConditions[0][1], $noteq_symbol) && !in_array($filterConditions[0][1], $in_symbol) && !in_array($filterConditions[0][1], $notin_symbol))) {
                    $this->throwWindException('过滤 ' . $fd . ' 字段需使用gt、lt、gte、lte、eq、in、noteq、notin等符号', 0);
                }
                $dir = $this->getStorageDir();
                $dir = $dir . 'baseData/' . $this->IndexName . '.db';
                $pdo = new PDO_sqlite($dir);
                if (is_array($foreachIds) && !empty($foreachIds) && count($foreachIds) < $this->filterFunHhreshold) {
                    $filterConditionsLen = count($filterConditions);
                    if ($filterConditionsLen == 1) {
                        $fd = $filterConditions[0][0];
                        $symbol = $filterConditions[0][1];
                        $val = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                    } else {
                        $fd = $filterConditions[0][0];
                        $symbol1 = $filterConditions[0][1];
                        $symbol2 = $filterConditions[1][1];
                        $val1 = (float)$filterConditions[0][2];
                        $val2 = (float)$filterConditions[1][2];
                    }
                    $countContainer = [];
                    $primarykey = $this->mapping['properties']['primarykey'];

                    $foreachIdsCount = count($foreachIds);
                    $limit = $this->getOriginalSourceSize;
                    $step = ceil($foreachIdsCount / $limit);
                    for ($i = 0; $i < $step; ++$i) {
                        $ids = array_slice($foreachIds, $i * $limit, $limit);
                        $ids = "'" . implode("','", (array)$ids) . "'";
                        $sql = "select * from $this->IndexName where $primarykey in($ids);";
                        $resAll = $pdo->getAll($sql);
                        if ($resAll) {
                            foreach ($resAll as $resRow) {
                                $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                if ($filterConditionsLen == 1) {

                                    $primarykeyVal = $resRow[$primarykey];
                                    if ($symbol == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent < $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent <= $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent > $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent >= $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'eq') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent == $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'noteq') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent != $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'in') {
                                        $fieldContent = $resRow[$fd];
                                        if (isset($val[$fieldContent . '_'])) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'notin') {
                                        $fieldContent = $resRow[$fd];
                                        if (!isset($val[$fieldContent . '_'])) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    }
                                } else if ($filterConditionsLen == 2) {

                                    $primarykeyVal = $resRow[$primarykey];
                                    if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!empty($countContainer)) {
                        $foreachIds = $countContainer;
                    } else {
                        return [];
                    }
                } else {
                    $this->getDateIntervalData($fd);
                    $countContainer = [];
                    $filterConditionsLen = count($filterConditions);
                    $symbolArr = [];
                    $valArr = [];
                    if ($filterConditionsLen == 1) {
                        $fd = $filterConditions[0][0];
                        $symbolArr[] = $filterConditions[0][1];
                        $valArr[] = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                    } else {
                        $fd = $filterConditions[0][0];
                        $symbolArr[] = $filterConditions[0][1];
                        $symbolArr[] = $filterConditions[1][1];
                        $valArr[] = (float)$filterConditions[0][2];
                        $valArr[] = (float)$filterConditions[1][2];
                    }
                    if ($filterConditionsLen == 1) {
                        $symbol = $filterConditions[0][1];
                        $val = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                    } else {
                        $symbol1 = $filterConditions[0][1];
                        $symbol2 = $filterConditions[1][1];
                        $val1 = (float)$filterConditions[0][2];
                        $val2 = (float)$filterConditions[1][2];
                    }
                    $intervalIds = [];
                    $intervalIdsCurr = [];
                    $needCompBaseList = [];
                    $realTimeNeedcomp = [];

                    if ($filterConditionsLen == 1) {
                        if (in_array($symbol, $comparison_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);

                            $interval = $this->getNumericInterval($val, $fd);

                            $intervalContainer = [];
                            $intervalCurrContainer = [];
                            $arrBounds = ['l', 'r'];
                            if ($symbol == 'gt' || $symbol == 'gte') {
                                if (in_array($interval, $arrBounds)) {
                                    if ($interval === 'l') {
                                        $intervalContainer = array_keys($this->interval_mapping[$fd]);
                                        $intervalContainer[] = 'r';
                                        $intervalCurrContainer = 'l';
                                    } else if ($interval === 'r') {
                                        $intervalContainer = [];
                                        $intervalCurrContainer = 'r';
                                    }
                                } else {
                                    if ($interval === false) {
                                        $intervalVal = (float)date('Ymd', $val);
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($v > $intervalVal) {
                                                $intervalContainer[] = $k;
                                            }
                                        }
                                        if (!empty($intervalContainer)) {
                                            $intervalContainer[] = 'r';
                                        } else {
                                            $intervalCurrContainer = 'r';
                                        }
                                    } else {
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($k == $interval) {
                                                $intervalCurrContainer = $k;
                                            }
                                            if ($k > $interval) {
                                                $intervalContainer[] = $k;
                                            }
                                        }
                                        $intervalContainer[] = 'r';
                                    }
                                }
                            } else if ($symbol == 'lt' || $symbol == 'lte') {
                                if (in_array($interval, $arrBounds)) {
                                    if ($interval == 'l') {
                                        $intervalCurrContainer = 'l';
                                        $intervalContainer = [];
                                    } else if ($interval == 'r') {
                                        $intervalContainer = array_keys($this->interval_mapping[$fd]);
                                        $intervalContainer[] = 'l';
                                        $intervalCurrContainer = 'r';
                                    }
                                } else {
                                    if ($interval === false) {
                                        $intervalVal = (float)date('Ymd', $val);
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($v < $intervalVal) {
                                                $intervalContainer[] = $k;
                                            }
                                        }
                                        if (!empty($intervalContainer)) {
                                            $intervalContainer[] = 'l';
                                        } else {
                                            $intervalCurrContainer = 'l';
                                        }
                                    } else {
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($k == $interval) {
                                                $intervalCurrContainer = $k;
                                            }
                                            if ($k < $interval) {
                                                $intervalContainer[] = $k;
                                            }
                                        }
                                        $intervalContainer[] = 'l';
                                    }
                                }
                            }



                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                if (is_array($intervalIdsCurr)) {
                                    $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                    $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                }
                            }
                            $realtime = $this->getRealtimeNumeric($fd, [$intervalCurrContainer]);
                            if (!empty($realtime)) {
                                $list = [];
                                foreach ($realtime as $k => $v) {
                                    $list[] = $v[1];
                                }
                            }
                            $realTimeNeedcomp = $list;
                        } else if (in_array($symbol, $eq_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);

                            $interval = $this->getNumericInterval($val, $fd);

                            $intervalCurrContainer = $interval;
                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                if (is_array($intervalIdsCurrTemp)) {
                                    $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                    $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                }
                            }
                            $realtime = $this->getRealtimeNumeric($fd, [$intervalCurrContainer]);
                            if (!empty($realtime)) {
                                $list = [];
                                foreach ($realtime as $k => $v) {
                                    $list[] = $v[1];
                                }
                            }
                            $realTimeNeedcomp = $list;
                        } else if (in_array($symbol, $noteq_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);

                            $interval = $this->getNumericInterval($val, $fd);

                            $intervalContainer = [];
                            $intervalCurrContainer = '';
                            foreach ($this->interval_mapping[$fd] as $k => $v) {
                                if ($k == $interval) {
                                    $intervalCurrContainer = $k;
                                } else {
                                    $intervalContainer[] = $k;
                                }
                            }



                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                if (is_array($intervalIdsCurrTemp)) {
                                    $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                    $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                }
                            }
                            $realtime = $this->getRealtimeNumeric($fd, [$intervalCurrContainer]);
                            if (!empty($realtime)) {
                                $list = [];
                                foreach ($realtime as $k => $v) {
                                    $list[] = $v[1];
                                }
                            }
                            $realTimeNeedcomp = $list;
                        } else if (in_array($symbol, $in_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $intervalCurrContainer = [];
                            if (is_array($val)) {
                                foreach ($val as $n => $m) {

                                    $interval = $this->getNumericInterval((float)mb_substr($n, 0, -1), $fd);

                                    foreach ($this->interval_mapping[$fd] as $k => $v) {
                                        if ($k == $interval) {
                                            $intervalCurrContainer[] = $k;
                                        }
                                    }
                                }
                                $intervalCurrContainer = array_unique($intervalCurrContainer);
                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                        if (is_array($intervalIdsCurrTemp)) {
                                            $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                            $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                        }
                                    }
                                }
                                $realtime = $this->getRealtimeNumeric($fd, $intervalCurrContainer);
                                if (!empty($realtime)) {
                                    $list = [];
                                    foreach ($realtime as $k => $v) {
                                        $list[] = $v[1];
                                    }
                                }
                                $realTimeNeedcomp = $list;
                            }
                        } else if (in_array($symbol, $notin_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $intervalContainer = [];
                            $intervalCurrContainer = [];
                            if (is_array($val)) {
                                foreach ($val as $n => $m) {

                                    $interval = $this->getNumericInterval((float)mb_substr($n, 0, -1), $fd);
                                    foreach ($this->interval_mapping[$fd] as $k => $v) {
                                        if ($k == $interval) {
                                            $intervalCurrContainer[] = $k;
                                        } else {
                                            $intervalContainer[] = $k;
                                        }
                                    }
                                }



                                $intervalCurrContainer = array_unique($intervalCurrContainer);
                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                        if (is_array($intervalIdsCurrTemp)) {
                                            $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                            $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                        }
                                    }
                                }
                                $realtime = $this->getRealtimeNumeric($fd, $intervalCurrContainer);
                                if (!empty($realtime)) {
                                    $list = [];
                                    foreach ($realtime as $k => $v) {
                                        $list[] = $v[1];
                                    }
                                }
                                $realTimeNeedcomp = $list;
                            }
                        }
                    } else {
                        if (in_array($symbol1, $comparison_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);

                            $interval1 = $this->getNumericInterval($val1, $fd);
                            $interval2 = $this->getNumericInterval($val2, $fd);

                            $intervalContainer1 = [];
                            $intervalContainer2 = [];
                            $intervalCurrContainer = [];
                            $arrBounds = ['l', 'r'];
                            if ($interval1 !== $interval2) {
                                if ($symbol1 == 'gt' || $symbol1 == 'gte') {
                                    if (in_array($interval1, $arrBounds)) {
                                        if ($interval1 == 'l') {
                                            $intervalContainer1 = array_keys($this->interval_mapping[$fd]);
                                            $intervalContainer1[] = 'r';
                                            $intervalCurrContainer[] = 'l';
                                        } else if ($interval1 == 'r') {
                                            $intervalContainer1 = [];
                                            $intervalCurrContainer[] = 'r';
                                        }
                                    } else {
                                        $intervalCurrContainer[] = $interval1;
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($k > $interval1) {
                                                $intervalContainer1[] = $k;
                                            }
                                        }
                                        $intervalContainer1[] = 'r';
                                    }
                                }
                                if ($symbol2 == 'lt' || $symbol2 == 'lte') {
                                    if (in_array($interval2, $arrBounds)) {
                                        if ($interval2 == 'l') {
                                            $intervalCurrContainer[] = 'l';
                                            $intervalContainer2 = [];
                                        } else if ($interval2 == 'r') {
                                            $intervalContainer2 = array_keys($this->interval_mapping[$fd]);
                                            $intervalContainer2[] = 'l';
                                            $intervalCurrContainer[] = 'r';
                                        }
                                    } else {
                                        $intervalCurrContainer[] = $interval2;
                                        foreach ($this->interval_mapping[$fd] as $k => $v) {
                                            if ($k < $interval2) {
                                                $intervalContainer2[] = $k;
                                            }
                                        }
                                        $intervalContainer2[] = 'l';
                                    }
                                }
                                $intervalContainer = array_intersect($intervalContainer1, $intervalContainer2);



                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in ($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                        if (is_array($intervalIdsCurrTemp)) {
                                            $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                            $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                        }
                                    }
                                }
                                $realtime = $this->getRealtimeNumeric($fd, $intervalCurrContainer);
                                if (!empty($realtime)) {
                                    $list = [];
                                    foreach ($realtime as $k => $v) {
                                        $list[] = $v[1];
                                    }
                                }
                                $realTimeNeedcomp = $list;
                            } else {
                                $intervalCurrContainer = [];
                                $intervalCurrContainer[] = $interval1;
                                $intervalContainer = [];





                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in ($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $intervalIdsCurrTemp = json_decode($ids_gzinf, true);
                                        if (is_array($intervalIdsCurrTemp)) {
                                            $needCompBaseList[] = $intervalIdsCurrTemp[0];
                                            $intervalIdsCurr[] = $intervalIdsCurrTemp[1];
                                        }
                                    }
                                }
                                $realtime = $this->getRealtimeNumeric($fd, $intervalCurrContainer);
                                if (!empty($realtime)) {
                                    $list = [];
                                    foreach ($realtime as $k => $v) {
                                        $list[] = $v[1];
                                    }
                                }
                                $realTimeNeedcomp = $list;
                            }
                        }
                    }
                    if (!empty($realTimeNeedcomp)) {
                        foreach ($realTimeNeedcomp as $k => $arr) {
                            foreach ($arr as $n => $str) {
                                foreach ($arr[0] as $n => $str) {
                                    $this->compare($countContainer, $filterConditionsLen, $n, $symbolArr, $valArr, $str);
                                }
                            }
                        }
                    }

                    if (!empty($needCompBaseList)) {
                        foreach ($needCompBaseList as $k => $arr) {
                            foreach ($arr as $n => $str) {
                                $this->compare($countContainer, $filterConditionsLen, $n, $symbolArr, $valArr, $str);
                            }
                        }
                        if (!empty($countContainer)) {
                            $countContainer = explode(',', implode(',', $countContainer));
                        }
                        $intervalIds = [];
                        $intervalIds = $this->getNumericNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer);
                        $mergeRes = (array)array_unique(array_merge($countContainer, $intervalIds));
                        if (is_array($foreachIds)) {
                            $intersection = $this->multi_skip_intersection($mergeRes, $foreachIds);
                            if (!empty($intersection)) {
                                $foreachIds = $intersection;
                            } else {
                                return [];
                            }
                        } else {
                            if (!empty($mergeRes)) {
                                $foreachIds = $mergeRes;
                            } else {
                                return [];
                            }
                        }
                    } else {
                        if (!empty($countContainer)) {
                            $countContainer = explode(',', implode(',', $countContainer));
                        }
                        $intervalIds = [];
                        $intervalIds = $this->getNumericNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer);
                        if (!empty($intervalIds)) {
                            if (!empty($countContainer)) {
                                $intervalIds = array_merge($intervalIds, $countContainer);
                            }
                            if (is_array($foreachIds)) {
                                $foreachIds = $this->multi_skip_intersection($intervalIds, $foreachIds);
                                if (!empty($foreachIds)) {
                                    $foreachIds = $foreachIds;
                                } else {
                                    return [];
                                }
                            } else {
                                $foreachIds = $intervalIds;
                            }
                        } else {
                            return [];
                        }
                    }
                }
            } else if ($fieldType === 'date') {
                if (!isset($filterConditions[0][1]) || (!in_array($filterConditions[0][1], $comparison_symbol) && !in_array($filterConditions[0][1], $eq_symbol) && !in_array($filterConditions[0][1], $noteq_symbol) && !in_array($filterConditions[0][1], $in_symbol) && !in_array($filterConditions[0][1], $notin_symbol))) {
                    $this->throwWindException('过滤 ' . $fd . ' 字段需使用gt、lt、gte、lte、eq、in、noteq、notin等符号', 0);
                }
                if ($fieldType === 'date') {
                    if ((count($filterConditions) == 1)) {
                        if ((in_array($filterConditions[0][1], $comparison_symbol) || in_array($filterConditions[0][1], $eq_symbol) || in_array($filterConditions[0][1], $noteq_symbol))) {
                            if ($this->isValidDateString($filterConditions[0][2])) {
                                $filterConditionsStr[0] = $filterConditions[0][2];
                                if ($this->isNYRDateString($filterConditions[0][2])) {
                                    if ($filterConditions[0][1] === 'gt') {
                                        $filterConditions[0][2] = strtotime($filterConditions[0][2]) + 3600 * 24;
                                    } else {
                                        $filterConditions[0][2] = strtotime($filterConditions[0][2]);
                                    }
                                } else {
                                    $filterConditions[0][2] = strtotime($filterConditions[0][2]);
                                }
                            } else {
                                $filterConditions[0][2] = (int)$filterConditions[0][2];
                            }
                        }
                    } else {
                        if (in_array($filterConditions[0][1], $comparison_symbol)) {
                            if ($this->isValidDateString($filterConditions[0][2])) {
                                $filterConditionsStr[0] = $filterConditions[0][2];
                                if ($this->isNYRDateString($filterConditions[0][2])) {
                                    if ($filterConditions[0][1] == 'gt') {
                                        $filterConditions[0][2] = strtotime($filterConditions[0][2]) + 3600 * 24;
                                    } else {
                                        $filterConditions[0][2] = strtotime($filterConditions[0][2]);
                                    }
                                } else {
                                    $filterConditions[0][2] = strtotime($filterConditions[0][2]);
                                }
                            } else {
                                $filterConditions[0][2] = (int)$filterConditions[0][2];
                            }
                            if ($this->isValidDateString($filterConditions[1][2]) && in_array($filterConditions[1][1], $comparison_symbol)) {
                                $filterConditionsStr[1] = $filterConditions[1][2];
                                if ($this->isNYRDateString($filterConditions[1][2])) {
                                    if ($filterConditions[1][1] == 'lte') {
                                        $filterConditions[1][2] = strtotime($filterConditions[1][2]) + 3600 * 24;;
                                    } else {
                                        $filterConditions[1][2] = strtotime($filterConditions[1][2]);
                                    }
                                } else {
                                    $filterConditions[1][2] = strtotime($filterConditions[1][2]);
                                }
                            } else {
                                $filterConditions[1][2] = (int)$filterConditions[1][2];
                            }
                        }
                    }
                }
                $dir = $this->getStorageDir();
                $dir = $dir . 'baseData/' . $this->IndexName . '.db';
                $pdo = new PDO_sqlite($dir);
                if (is_array($foreachIds) && !empty($foreachIds) && count($foreachIds) < $this->filterFunHhreshold) {
                    $filterConditionsLen = count($filterConditions);
                    if ($filterConditionsLen == 1) {
                        $fd = $filterConditions[0][0];
                        $symbol = $filterConditions[0][1];
                        $val = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                    } else {
                        $fd = $filterConditions[0][0];
                        $symbol1 = $filterConditions[0][1];
                        $symbol2 = $filterConditions[1][1];
                        $val1 = (float)$filterConditions[0][2];
                        $val2 = (float)$filterConditions[1][2];
                    }
                    $countContainer = [];
                    $primarykey = $this->mapping['properties']['primarykey'];

                    $foreachIdsCount = count($foreachIds);
                    $limit = $this->getOriginalSourceSize;
                    $step = ceil($foreachIdsCount / $limit);
                    for ($i = 0; $i < $step; ++$i) {
                        $ids = array_slice($foreachIds, $i * $limit, $limit);
                        $ids = "'" . implode("','", (array)$ids) . "'";
                        $sql = "select * from $this->IndexName where $primarykey in($ids);";
                        $resAll = $pdo->getAll($sql);
                        if ($resAll) {
                            foreach ($resAll as $resRow) {
                                $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                if ($filterConditionsLen == 1) {

                                    $primarykeyVal = $resRow[$primarykey];
                                    if ($symbol == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent < $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent <= $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent > $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent >= $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'eq') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent == $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'noteq') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if ($fieldContent != $val) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'in') {
                                        $fieldContent = $resRow[$fd];
                                        if (isset($val[$fieldContent . '_'])) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol == 'notin') {
                                        $fieldContent = $resRow[$fd];
                                        if (!isset($val[$fieldContent . '_'])) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    }
                                } else if ($filterConditionsLen == 2) {

                                    $primarykeyVal = $resRow[$primarykey];
                                    if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                                        $fieldContent = (float)$resRow[$fd];
                                        if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                                            $countContainer[] = $primarykeyVal;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!empty($countContainer)) {
                        $foreachIds = $countContainer;
                    } else {
                        return [];
                    }
                } else {
                    $filterConditionsLen = count($filterConditions);
                    if ($filterConditionsLen == 1) {
                        $fd = $filterConditions[0][0];
                        $symbol = $filterConditions[0][1];
                        $val = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                    } else {
                        $fd = $filterConditions[0][0];
                        $symbol1 = $filterConditions[0][1];
                        $symbol2 = $filterConditions[1][1];
                        $val1 = (float)$filterConditions[0][2];
                        $val2 = (float)$filterConditions[1][2];
                    }
                    $this->getDateIntervalData($fd);
                    $intervalIds = [];
                    $intervalIdsCurr = [];

                    if ($filterConditionsLen == 1) {
                        if (in_array($symbol, $comparison_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $interval = $this->getDateNYR($val);
                            $intervalContainer = [];
                            $intervalCurrContainer = $interval;
                            if ($symbol == 'gt' || $symbol == 'gte') {
                                foreach ($this->interval_mapping[$fd] as $k => $v) {
                                    if ($v > $interval) {
                                        $intervalContainer[] = $v;
                                    }
                                }
                            } else if ($symbol == 'lt' || $symbol == 'lte') {
                                foreach ($this->interval_mapping[$fd] as $k => $v) {
                                    if ($v < $interval) {
                                        $intervalContainer[] = $k;
                                    }
                                }
                            }




                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                $intervalIdsCurr = explode(',', $ids_gzinf);
                            }
                            if ($intervalCurrContainer !== '') {
                                $realtime = $this->getRealtimeDate($fd, [$intervalCurrContainer]);
                                $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                            }
                            if ($this->isNYRDateString($filterConditionsStr[0]) && count($filterConditionsStr) == 1) {
                                if (is_string($intervalCurrContainer)) {
                                    if ($filterConditionsStr[0][1] == 'gt' || $filterConditionsStr[0][1] == 'gte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    } else if ($filterConditionsStr[0][1] == 'lte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    }
                                }
                            }
                        } else if (in_array($symbol, $eq_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $interval = $this->getDateNYR($val);
                            $intervalCurrContainer = $interval;
                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                $intervalIdsCurr = explode(',', $ids_gzinf);
                            }
                            if ($intervalCurrContainer !== '') {
                                $realtime = $this->getRealtimeDate($fd, [$intervalCurrContainer]);
                                $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                            }
                            if ($this->isNYRDateString($filterConditionsStr[0]) && count($filterConditionsStr) == 1) {
                                if (is_string($intervalCurrContainer)) {
                                    if ($filterConditionsStr[0][1] == 'gt' || $filterConditionsStr[0][1] == 'gte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    } else if ($filterConditionsStr[0][1] == 'lte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    }
                                }
                            }
                        } else if (in_array($symbol, $noteq_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $interval = $this->getDateNYR($val);
                            $intervalContainer = [];
                            $intervalCurrContainer = $interval;
                            foreach ($this->interval_mapping[$fd] as $k => $v) {
                                if ($k != $interval) {
                                    $intervalContainer[] = $v;
                                }
                            }




                            $intervalIdsCurr = [];
                            $sql = "select * from $field_interval where term = '$intervalCurrContainer';";
                            $resRow = $pdo_interval->getRow($sql);
                            if ($resRow) {
                                $ids = $resRow['ids'];
                                $ids_gzinf = $this->systemDecompression($ids);
                                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                $intervalIdsCurr = explode(',', $ids_gzinf);
                            }
                            if ($intervalCurrContainer !== '') {
                                $realtime = $this->getRealtimeDate($fd, [$intervalCurrContainer]);
                                $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                            }
                            if ($this->isNYRDateString($filterConditionsStr[0]) && count($filterConditionsStr) == 1) {
                                if (is_string($intervalCurrContainer)) {
                                    if ($filterConditionsStr[0][1] == 'gt' || $filterConditionsStr[0][1] == 'gte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    } else if ($filterConditionsStr[0][1] == 'lte') {
                                        $intervalIds = array_merge($intervalIds, $intervalIdsCurr);
                                        $intervalIdsCurr = [];
                                    }
                                }
                            }
                        } else if (in_array($symbol, $in_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $intervalCurrContainer = [];
                            if (is_array($val)) {
                                foreach ($val as $n => $m) {
                                    $interval = $this->getDateNYR(strtotime(mb_substr($n, 0, -1)));
                                    $intervalCurrContainer[] = $interval;
                                }
                                $intervalCurrContainer = array_unique($intervalCurrContainer);
                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                        $intervalIdsCurr[] = $ids_gzinf;
                                    }
                                    $intervalIdsCurr = explode(',', implode(',', $intervalIdsCurr));
                                }
                                if (!empty($intervalCurrContainer)) {
                                    $realtime = $this->getRealtimeDate($fd, $intervalCurrContainer);
                                    $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                                    $intervalIds = $intervalIdsCurr;
                                    $intervalIdsCurr = [];
                                }
                            }
                        } else if (in_array($symbol, $notin_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $intervalContainer = [];
                            $intervalCurrContainer = [];
                            if (is_array($val)) {
                                $intervalTemp = [];
                                foreach ($val as $n => $m) {
                                    $interval = $this->getDateNYR(strtotime(mb_substr($n, 0, -1)));
                                    $intervalCurrContainer[] = $interval;
                                    $intervalTemp[] = $interval;
                                }
                                foreach ($this->interval_mapping[$fd] as $k => $v) {
                                    if (!in_array($v, $intervalTemp)) {
                                        $intervalContainer[] = $v;
                                    }
                                }
                            }
                        }
                    } else {
                        if (in_array($symbol1, $comparison_symbol)) {
                            $field_interval = $fd . '_interval';
                            $dir = $this->indexDir . $this->IndexName . '/index/' . $field_interval . '.db';
                            $pdo_interval = new PDO_sqlite($dir);
                            $interval1 = $this->getDateNYR($val1);
                            $interval2 = $this->getDateNYR($val2);
                            $intervalContainer1 = [];
                            $intervalContainer2 = [];
                            $intervalCurrContainer = [];
                            if ($interval1 !== $interval2) {
                                $intervalCurrContainer = [$interval1, $interval2];
                                if ($symbol1 == 'gt' || $symbol1 == 'gte') {
                                    foreach ($this->interval_mapping[$fd] as $k => $v) {
                                        if ($v > $interval1) {
                                            $intervalContainer1[] = $v;
                                        }
                                    }
                                }
                                if ($symbol2 == 'lt' || $symbol2 == 'lte') {
                                    foreach ($this->interval_mapping[$fd] as $k => $v) {
                                        if ($v < $interval2) {
                                            $intervalContainer2[] = $v;
                                        }
                                    }
                                }
                                $intervalContainer = array_intersect($intervalContainer1, $intervalContainer2);
                                if ($this->isNYRDateString($filterConditionsStr[0]) && $this->isNYRDateString($filterConditionsStr[1])) {
                                    if (is_array($intervalCurrContainer) && count($intervalCurrContainer) == 2) {
                                        if ($filterConditions[0][1] == 'gt' || $filterConditions[0][1] == 'gte') {
                                            $intervalContainer[] = $intervalCurrContainer[0];
                                        } else if ($filterConditions[1][1] == 'lte') {
                                            $intervalContainer[] = $intervalCurrContainer[1];
                                        }
                                        $intervalCurrContainer = [];
                                    }
                                }
                                $intervalContainer = array_unique($intervalContainer);







                                $intervalIdsCurr = [];
                                $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                $sql = "select * from $field_interval where term in ($intervalStr);";
                                $resAll = $pdo_interval->getAll($sql);
                                if ($resAll) {
                                    foreach ($resAll as $resRow) {
                                        $ids = $resRow['ids'];
                                        $ids_gzinf = $this->systemDecompression($ids);
                                        $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                        $intervalIdsCurr[] = $ids_gzinf;
                                    }
                                    $intervalIdsCurr = explode(',', implode(',', $intervalIdsCurr));
                                }
                                if (!empty($intervalCurrContainer)) {
                                    $realtime = $this->getRealtimeDate($fd, $intervalCurrContainer);
                                    $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                                }
                            } else {
                                if ($this->isNYRDateString($filterConditionsStr[0]) && $this->isNYRDateString($filterConditionsStr[1])) {
                                    if (is_array($intervalCurrContainer) && count($intervalCurrContainer) == 2) {
                                        $intervalIdsCurr = [];
                                        $intervalIds = [];
                                    }
                                } else {
                                    $intervalCurrContainer[] = $interval1;
                                    $intervalContainer = [];








                                    $intervalIdsCurr = [];
                                    $intervalStr = "'" . implode("','", $intervalCurrContainer) . "'";
                                    $sql = "select * from $field_interval where term in ($intervalStr);";
                                    $resAll = $pdo_interval->getAll($sql);
                                    if ($resAll) {
                                        foreach ($resAll as $resRow) {
                                            $ids = $resRow['ids'];
                                            $ids_gzinf = $this->systemDecompression($ids);
                                            $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                            $intervalIdsCurr[] = $ids_gzinf;
                                        }
                                        $intervalIdsCurr = explode(',', implode(',', $intervalIdsCurr));
                                    }
                                    if (!empty($intervalCurrContainer)) {
                                        $realtime = $this->getRealtimeDate($fd, $intervalCurrContainer);
                                        $intervalIdsCurr = array_merge($intervalIdsCurr, $realtime);
                                    }
                                }
                            }
                        }
                    }

                    if (!empty($intervalIdsCurr)) {
                        if (is_array($foreachIds) && !empty($foreachIds)) {
                            if (!empty($intervalIdsCurr)) {
                                $intervalIdsCurr = $this->multi_skip_intersection($intervalIdsCurr, $foreachIds);
                            }
                        }
                        $countContainer = [];
                        $primarykey = $this->mapping['properties']['primarykey'];
                        $intervalIdsCurrCount = count($intervalIdsCurr);
                        $limit = $this->getOriginalSourceSize;
                        $step = ceil($intervalIdsCurrCount / $limit);
                        for ($i = 0; $i < $step; ++$i) {
                            $ids = array_slice($intervalIdsCurr, $i * $limit, $limit);
                            $ids = "'" . implode("','", (array)$ids) . "'";
                            $sql = "select * from $this->IndexName where $primarykey in($ids);";
                            $resAll = $pdo->getAll($sql);
                            if ($resAll) {
                                foreach ($resAll as $resRow) {
                                    $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                    if ($filterConditionsLen == 1) {
                                        $primarykeyVal = $resRow[$primarykey];
                                        if ($symbol == 'lt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent < $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'lte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent <= $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'gt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent > $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'gte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent >= $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'eq') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent == $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'noteq') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if ($fieldContent != $val) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'in') {
                                            $fieldContent = $resRow[$fd];
                                            $fieldContent = date('Y-m-d', $fieldContent);
                                            if (isset($val[$fieldContent . '_'])) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol == 'notin') {
                                            $fieldContent = $resRow[$fd];
                                            $fieldContent = date('Y-m-d', $fieldContent);
                                            if (!isset($val[$fieldContent . '_'])) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        }
                                    } else if ($filterConditionsLen == 2) {

                                        $primarykeyVal = $resRow[$primarykey];
                                        if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                                            $fieldContent = (float)$resRow[$fd];
                                            if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                                                $countContainer[] = $primarykeyVal;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        $intervalIds = [];
                        $intervalIds = $this->getDateNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer);

                        if (is_array($foreachIds)) {
                            $intervalIds = $this->multi_skip_intersection($intervalIds, $foreachIds);
                            $mergeRes = array_merge($countContainer, $intervalIds);
                            if (!empty($mergeRes)) {
                                $foreachIds = $mergeRes;
                            } else {
                                return [];
                            }
                        } else {
                            $mergeRes = array_merge($countContainer, $intervalIds);
                            if (!empty($mergeRes)) {
                                $foreachIds = $countContainer;
                            } else {
                                return [];
                            }
                        }
                    } else {
                        $intervalIds = [];
                        $intervalIds = $this->getDateNoNeedCompIds($fd, $pdo_interval, $field_interval, $intervalContainer);
                        if (!empty($intervalIds)) {
                            if (is_array($foreachIds)) {
                                $foreachIds = $this->multi_skip_intersection($intervalIds, $foreachIds);
                                if (!empty($foreachIds)) {
                                    $foreachIds = $foreachIds;
                                } else {
                                    return [];
                                }
                            } else {
                                $foreachIds = $intervalIds;
                            }
                        } else {
                            return [];
                            $countContainer = [];
                            $primarykey = $this->mapping['properties']['primarykey'];
                            $min_max_id = $this->get_minid_maxid($pdo);
                            if ($min_max_id) {
                                $minId = $min_max_id[0];
                                $maxid = $min_max_id[1];
                                $docNum = $min_max_id[2];
                                $step = ceil($docNum / 500);
                                for ($i = 1; $i < $step; ++$i) {
                                    $b1 = $minId + ($i - 1) * 500;
                                    $b2 = $minId + $i * 500 - 1;
                                    $sql = "select * from $this->IndexName where $this->sys_primarykey between $b1 and $b2;";
                                    $resAll = $pdo->getAll($sql);
                                    if ($resAll) {
                                        foreach ($resAll as $resRow) {
                                            $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                            if ($filterConditionsLen == 1) {
                                                $primarykeyVal = $resRow[$primarykey];
                                                if ($symbol == 'lt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent < $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'lte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent <= $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'gt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent > $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'gte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent >= $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'eq') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent == $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'noteq') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if ($fieldContent != $val) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'in') {
                                                    $fieldContent = $resRow[$fd];
                                                    if (isset($val[$fieldContent . '_'])) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol == 'notin') {
                                                    $fieldContent = $resRow[$fd];
                                                    if (!isset($val[$fieldContent . '_'])) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                }
                                            } else if ($filterConditionsLen == 2) {
                                                $primarykeyVal = $resRow[$primarykey];
                                                if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                                                    $fieldContent = (float)$resRow[$fd];
                                                    if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                                                        $countContainer[] = $primarykeyVal;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (!empty($countContainer)) {
                                if (is_array($foreachIds)) {
                                    $foreachIds = $this->multi_skip_intersection($foreachIds, $countContainer);
                                } else {
                                    $foreachIds = $countContainer;
                                }
                                if (empty($foreachIds)) {
                                    return [];
                                }
                            } else {
                                return [];
                            }
                        }
                    }
                }
            } else if ($fieldType == 'keyword' || $fieldType == 'text') {
                $isIndexField = $this->isIndexField($fd);
                if (!$isIndexField) {
                    $this->throwWindException($fd . ' 字段未配置索引', 0);
                }
                $fd = $filterConditions[0][0];
                $symbol = $filterConditions[0][1];
                $val = $filterConditions[0][2];
                if ($symbol !== 'match') {
                    $this->throwWindException('过滤 ' . $fd . ' 字段需使用match关键字', 0);
                }
                $match_prefix_list = [];
                $match_suffix_list = [];
                $match_complete_list = [];
                if (is_string($val)) {
                    $terms_list = [$val];
                } else if (is_array($val)) {
                    $terms_list = $val;
                } else {
                    $terms_list = [];
                }
                foreach ($terms_list as $t) {
                    if (mb_substr($t, -1) == '*') {
                        $match_prefix_list[] = mb_substr($t, 0, -1);
                    } else if (mb_substr($t, 0, 1) == '*') {
                        $match_suffix_list[] = mb_substr($t, 1);
                    } else {
                        $match_complete_list[] = $t;
                    }
                }

                $match_res = [];
                if (!empty($match_prefix_list)) {
                    $keywordIndexData = $this->filter_prefix($fd, $match_prefix_list);
                    if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                        $match_res[] = $keywordIndexData;
                    }
                }
                if (!empty($match_suffix_list)) {
                    $keywordIndexData = $this->filter_suffix($fd, $match_suffix_list);
                    if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                        $match_res[] = $keywordIndexData;
                    }
                }
                if (!empty($match_complete_list)) {
                    $keywordIndexData = $this->getKeywordAndTextIndexData($fd, $match_complete_list);
                    if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                        $match_res[] = $keywordIndexData;
                    }
                }
                if (!empty($match_res)) {
                    $match_res = (array)array_unique(array_merge(...$match_res));
                    if (is_array($foreachIds)) {
                        $foreachIds = $this->multi_skip_intersection($foreachIds, $match_res);
                    } else {
                        $foreachIds = $match_res;
                    }
                    if (empty($foreachIds)) {
                        return [];
                    }
                } else {
                    return [];
                }
            } else if ($fieldType == 'primarykey') {
                $primarykey_type = $this->primarykeyType;
                if ($primarykey_type === 'UUID') {
                    if (!isset($filterConditions[0][1]) || (!in_array($filterConditions[0][1], $in_symbol) && !in_array($filterConditions[0][1], $notin_symbol))) {
                        $this->throwWindException($fd . ' 字段是UUID主键，过滤此字段只能使用 in、notin 符号', 0);
                    }
                } else {
                    if (!isset($filterConditions[0][1]) || (!in_array($filterConditions[0][1], $comparison_symbol) && !in_array($filterConditions[0][1], $eq_symbol) && !in_array($filterConditions[0][1], $noteq_symbol) && !in_array($filterConditions[0][1], $in_symbol) && !in_array($filterConditions[0][1], $notin_symbol))) {
                        $this->throwWindException('过滤 ' . $fd . ' 字段需使用gt、lt、gte、lte、eq、in、noteq、notin等符号', 0);
                    }
                }
                $filterConditionsLen = count($filterConditions);
                if ($filterConditionsLen == 1) {
                    $fd = $filterConditions[0][0];
                    $symbol = $filterConditions[0][1];
                    $val = (is_array($filterConditions[0][2])) ? $filterConditions[0][2] : (float)$filterConditions[0][2];
                } else {
                    $fd = $filterConditions[0][0];
                    $symbol1 = $filterConditions[0][1];
                    $symbol2 = $filterConditions[1][1];
                    $val1 = (float)$filterConditions[0][2];
                    $val2 = (float)$filterConditions[1][2];
                }
                $countContainer = [];
                $primarykey = $this->mapping['properties']['primarykey'];
                if (is_array($foreachIds) && !empty($foreachIds)) {
                    $foreachIdsCount = count($foreachIds);
                    $limit = $this->getOriginalSourceSize;
                    $step = ceil($foreachIdsCount / $limit);
                    for ($i = 0; $i < $step; ++$i) {
                        $ids = array_slice($foreachIds, $i * $limit, $limit);
                        foreach ($ids as $d) {
                            $resRow = [
                                $primarykey => $d,
                            ];
                            if ($filterConditionsLen == 1) {

                                $primarykeyVal = $resRow[$primarykey];
                                if ($symbol == 'lt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent < $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'lte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent <= $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'gt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent > $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'gte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent >= $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'eq') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent == $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'noteq') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if ($fieldContent != $val) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'in') {
                                    $fieldContent = $resRow[$fd];
                                    if (isset($val[$fieldContent . '_'])) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol == 'notin') {
                                    $fieldContent = $resRow[$fd];
                                    if (!isset($val[$fieldContent . '_'])) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                }
                            } else if ($filterConditionsLen == 2) {

                                $primarykeyVal = $resRow[$primarykey];
                                if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                                    $fieldContent = (float)$resRow[$fd];
                                    if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                                        $countContainer[] = $primarykeyVal;
                                    }
                                }
                            }
                        }
                    }
                    if (!empty($countContainer)) {
                        $foreachIds = $countContainer;
                    } else {
                        return [];
                    }
                } else {
                    return [];
                }
            }
        }
        if (is_array($foreachIds) && !empty($foreachIds)) {
            return $foreachIds;
        } else {
            return [];
        }
    }
    private function compare(&$countContainer, $filterConditionsLen, $n, $symbol, $val, $str)
    {
        if ($filterConditionsLen == 1) {
            $symbol = $symbol[0];
            $val = $val[0];
            $primarykeyVal = $str;
            if ($symbol == 'lt') {
                $fieldContent = (float)$n;
                if ($fieldContent < $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'lte') {
                $fieldContent = (float)$n;
                if ($fieldContent <= $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'gt') {
                $fieldContent = (float)$n;
                if ($fieldContent > $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'gte') {
                $fieldContent = (float)$n;
                if ($fieldContent >= $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'eq') {
                $fieldContent = (float)$n;
                if ($fieldContent == $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'noteq') {
                $fieldContent = (float)$n;
                if ($fieldContent != $val) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'in') {
                $fieldContent = $n;
                if (isset($val[$fieldContent . '_'])) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol == 'notin') {
                $fieldContent = $n;
                if (!isset($val[$fieldContent . '_'])) {
                    $countContainer[] = $primarykeyVal;
                }
            }
        } else if ($filterConditionsLen == 2) {
            $symbol1 = $symbol[0];
            $symbol2 = $symbol[1];
            $val1 = $val[0];
            $val2 = $val[1];
            $primarykeyVal = $str;
            if ($symbol1 == 'lt' && $symbol2 == 'gt') {
                $fieldContent = (float)$n;
                if (($fieldContent < $val1) && ($fieldContent > $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'gt' && $symbol2 == 'lt') {
                $fieldContent = (float)$n;
                if (($fieldContent > $val1) && ($fieldContent < $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'lte' && $symbol2 == 'gte') {
                $fieldContent = (float)$n;
                if (($fieldContent <= $val1) && ($fieldContent >= $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'gte' && $symbol2 == 'lte') {
                $fieldContent = (float)$n;
                if (($fieldContent >= $val1) && ($fieldContent <= $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'lt' && $symbol2 == 'gte') {
                $fieldContent = (float)$n;
                if (($fieldContent < $val1) && ($fieldContent >= $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'lte' && $symbol2 == 'gt') {
                $fieldContent = (float)$n;
                if (($fieldContent <= $val1) && ($fieldContent > $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'gt' && $symbol2 == 'lte') {
                $fieldContent = (float)$n;
                if (($fieldContent > $val1) && ($fieldContent <= $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            } else if ($symbol1 == 'gte' && $symbol2 == 'lt') {
                $fieldContent = (float)$n;
                if (($fieldContent >= $val1) && ($fieldContent < $val2)) {
                    $countContainer[] = $primarykeyVal;
                }
            }
        }
    }

    private function matchBoolEntry($query)
    {
        $page = $query['page'];
        $listRows = $query['list_rows'];

        $cacheKey = $this->get_cachekey($query);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $resArrScore = $this->match_bool($query);
        $resArrScore = array_slice($resArrScore, 0, $this->searchResMinNum, true);
        $ids_all_score = $resArrScore;
        $id_arr = array_keys($resArrScore);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );

        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }
    private function getDataOfMinimumShouldMatch($minimum_should_match, $lenTerm, $idsScore)
    {
        if (substr($minimum_should_match, -1) === '%') {
            $minimum_should_match = ((float)$minimum_should_match) / 100;
            $ComparisonValue = $minimum_should_match * $lenTerm;
            $ComparisonValue = (($ComparisonValue > 2) && ($ComparisonValue < 3)) ? 3 : $ComparisonValue;
            if ($ComparisonValue > 1) {
                $idsScoreTemp = [];
                foreach ($idsScore as $k => $v) {
                    if ($v >= $ComparisonValue) {
                        $idsScoreTemp[$k] = $v;
                    }
                }
                $idsScore = $idsScoreTemp;
            }
        } else {
            $ComparisonValue = (int)$minimum_should_match;
            if ($ComparisonValue > 1) {
                $idsScoreTemp = [];
                foreach ($idsScore as $k => $v) {
                    if ($v >= $ComparisonValue) {
                        $idsScoreTemp[$k] = $v;
                    }
                }
                $idsScore = $idsScoreTemp;
            }
        }
        return ['comparisonValue' => $ComparisonValue, 'res' => $idsScore];
    }

    private function match_bool($query)
    {
        $bool = (array)$query['bool'];
        $typeArr = ['must', 'should', 'must_not', 'filter'];
        $boolTypes = array_keys($bool);
        if (!in_array('must', $boolTypes) && !in_array('should', $boolTypes)) {
            return [];
        }
        $boolRes = [];
        $isFilter = false;
        foreach ($bool as $boolType => $v) {
            $res = [];
            if (in_array($boolType, $typeArr)) {
                $fields = (array)array_column($v['match'], 'fd');
                $fieldNum = count($fields);
                $this->fieldNum = $fieldNum;
                if ($boolType == 'must') {
                    $queryList = [
                        'operator' => 'and',
                    ];
                    foreach ($v['match'] as $i => $info) {
                        $temp = [
                            'name' => $info['fd'],
                            'operator' => 'and',
                        ];
                        $tempMerge = $this->array_merge_recursive($temp, $info);
                        $queryList['field'][] = $tempMerge;
                        $this->queryList['field'][] = $tempMerge + $this->queryList['highlight'];
                    }
                    $res = $this->boolCoreSearch($queryList);
                    $boolRes[$boolType] = $res;
                } else if ($boolType == 'should') {
                    if (isset($v['match'])) {
                        $minimum_should_match = isset($v['minimum_should_match']) ? $v['minimum_should_match'] : 1;
                        $queryList = [
                            'operator' => 'or',
                        ];
                        foreach ($v['match'] as $fd => $info) {
                            $temp = [
                                'name' => $info['fd'],
                                'operator' => 'and',
                            ];
                            $tempMerge = $this->array_merge_recursive($temp, $info);
                            $queryList['field'][] = $tempMerge;
                        }
                        $res = $this->boolCoreSearch($queryList);
                        $lenField = count($queryList['field']);
                        $res = $this->getDataOfMinimumShouldMatch($minimum_should_match, $lenField, $res);
                    } else if (!empty($v)) {
                        $minimum_should_match = isset($v['minimum_should_match']) ? $v['minimum_should_match'] : 1;
                        $tempShouldRes = [];
                        foreach ($v as $l) {
                            if (is_array($l) && isset($l['bool'])) {
                                $tempShouldRes[] = $this->match_bool($l);
                            }
                        }
                        if (!empty($tempShouldRes)) {
                            $tempShouldRes = !empty($tempShouldRes) ? $this->array_merge_recursive(...$tempShouldRes) : [];
                            foreach ($tempShouldRes as $k => $mergeArr) {
                                if (is_array($mergeArr)) {
                                    $tempShouldRes[$k] = array_sum($mergeArr);
                                }
                            }
                            arsort($tempShouldRes);
                            $res = $tempShouldRes;
                        }
                        $lenField = count($tempShouldRes);
                        $res = $this->getDataOfMinimumShouldMatch($minimum_should_match, $lenField, $res);
                    }
                    $boolRes[$boolType] = $res['res'];
                } else if ($boolType == 'must_not') {
                    $queryList = [
                        'operator' => 'or',
                    ];
                    foreach ($v['match'] as $fd => $info) {
                        $temp = [
                            'name' => $info['fd'],
                            'operator' => 'and',
                        ];
                        $tempMerge = $this->array_merge_recursive($temp, $info);
                        $queryList['field'][] = $tempMerge;
                    }
                    $res = $this->boolCoreSearch($queryList);
                    $boolRes[$boolType] = $res;
                } else if ($boolType == 'filter') {
                    $isFilter = true;
                }
            }
        }
        $resArr = [];
        $resArrScore = [];
        if (isset($boolRes['must'])) {
            $resArr = array_keys($boolRes['must']);
            if (isset($boolRes['should'])) {
                $resArr = $this->multi_skip_intersection_bigdata($resArr, array_keys($boolRes['should']));
            }
            if (isset($boolRes['must_not'])) {
                $resArr = array_diff($resArr, array_keys($boolRes['must_not']));
            }
            if ($isFilter && !empty($resArr)) {

                $filterRes = $this->getFilterIds($v, $resArr);
                $resArr = $filterRes;
            }
            foreach ($resArr as $d) {
                $resArrScore[$d] = $boolRes['must'][$d];
            }
        } else if (isset($boolRes['should'])) {
            $resArr = array_keys($boolRes['should']);
            if (isset($boolRes['must_not'])) {
                $resArr = array_diff($resArr, array_keys($boolRes['must_not']));
            }
            if ($isFilter && !empty($resArr)) {
                $filterRes = $this->getFilterIds($v, $resArr);
                $resArr = $filterRes;
            }
            foreach ($resArr as $d) {
                $resArrScore[$d] = $boolRes['should'][$d];
            }
        }
        return $resArrScore;
    }

    private function isBeginningOfString($string, $baseString)
    {
        if ($string == '') {
            return false;
        }
        $string = strtolower($string);
        $stringLen = strlen($string);
        $substring = substr($baseString, 0, $stringLen);
        return $substring == $string;
    }

    private function isEndingOfString($string, $baseString)
    {
        if ($string == '') {
            return false;
        }
        $string = strtolower($string);
        $stringLen = strlen($string);
        $substring = substr($baseString, -$stringLen);
        return $substring == $string;
    }

    private function filter_prefix($field, $prefix_terms = [], $is_return_all = false, $is_contain_perfect_match = true)
    {
        $countContainer = [];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        $fieldtype = $this->getFieldType($field);
        if ($fieldtype !== 'keyword' && $fieldtype !== 'text') {
            $this->throwWindException($field . ' 字段数量类型为' . $fieldtype . ',无法进行字符串匹配。（字符串匹配过滤只适用于keyword、text数据类型）', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未配置索引', 0);
        }
        $isKeyWordField = false;
        if ($this->isKeyWordField($field)) {
            $isKeyWordField = true;
        }

        foreach ($prefix_terms as $prefix_term) {
            if (mb_strlen($prefix_term) > 2) {
                $sub = mb_substr($prefix_term, 0, 3);
            } else if (mb_strlen($prefix_term) > 1) {
                $sub = mb_substr($prefix_term, 0, 2);
            } else if (mb_strlen($prefix_term) > 0) {
                $sub = mb_substr($prefix_term, 0, 1);
            } else {
                $sub = '';
            }
            $dir = $this->indexDir . $this->IndexName . '/index/_prefix_index.db';
            $pdo_prefix_index = new PDO_sqlite($dir);
            $sql = "select ids from $field where term='$sub';";
            $resRow = $pdo_prefix_index->getRow($sql);
            $termArr = [];
            if ($resRow) {
                $termlist = (array)json_decode($resRow['ids'], true);
                foreach ($termlist as $term) {
                    if ($this->isBeginningOfString($prefix_term, $term)) {
                        if (!$is_contain_perfect_match) {
                            if ($prefix_term !== $term) {
                                $termArr[] = $term;
                            }
                        } else {
                            $termArr[] = $term;
                        }
                    }
                }
            }
            if (!empty($termArr)) {
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
                $pdo = new PDO_sqlite($dir);
                $terms_str = "'" . implode("','", $termArr) . "'";
                $sql = "select ids from $field where term in($terms_str);";
                $resAll = $pdo->getAll($sql);
                if ($resAll) {
                    foreach ($resAll as $resRow) {
                        if ($this->primarykeyType == 'Int_Incremental') {
                            if ($isKeyWordField) {
                                $ids_gzinf = $this->systemDecompression($resRow['ids']);
                                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                $countContainer[] =    $ids_gzinf;
                            } else {
                                $resRow['ids'] = $this->systemDecompression($resRow['ids']);
                                $line = explode('/', $resRow['ids']);
                                $line[0] = $this->differentialDecompression($line[0]);
                                $shang = explode(',', $line[0]);
                                $ids = explode(',', $line[1]);
                                $bitmap = array_combine($shang, $ids);
                                $temp = [];
                                foreach ($bitmap as $s => $i) {
                                    $decimalArr = $this->bitmapInverse($s, $i);
                                    foreach ($decimalArr as $e) {
                                        $temp[] = $e;
                                    }
                                }
                                if (!empty($temp)) {
                                    $countContainer[] = implode(',', $temp);
                                }
                            }
                        } else {
                            $ids_gzinf = $this->systemDecompression($resRow['ids']);
                            $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                            $countContainer[] = $ids_gzinf;
                        }
                    }
                }
            }
            $termArrTemp = [];
            $idsArrTemp = [];
            $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
            if (is_file($dir . '/terms')) {
                $terms = json_decode(file_get_contents($dir . '/terms'), true);
                $terms = (array)$terms[0];
                $firstLetter = $this->getFirstLetter($prefix_term);
                $currFirstLetterTerms = isset($terms[$firstLetter]) ? $terms[$firstLetter] : [];
                foreach ($currFirstLetterTerms as $t) {
                    if ($this->isBeginningOfString($prefix_term, $t)) {
                        $termArrTemp[] = $t;
                    }
                }
                if (!empty($termArrTemp)) {
                    foreach ($termArrTemp as $t) {
                        $realTimeData = $this->getRealTimeData($field, $t);
                        if ($realTimeData !== false) {
                            $idsArrTemp[] = $realTimeData;
                        }
                    }
                }
            }
            if (!empty($idsArrTemp)) {
                $countContainer[] = implode(',', $idsArrTemp);
            }
        }
        if (!empty($countContainer)) {
            $countContainer = explode(',', implode(',', $countContainer));
            if ($is_return_all) {
                return $countContainer;
            } else {
                return array_unique($countContainer);
            }
        } else {
            return [];
        }
    }

    private function filter_suffix($field, $suffix_terms = [], $is_return_all = false, $is_contain_perfect_match = true)
    {
        $countContainer = [];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        $fieldtype = $this->getFieldType($field);
        if ($fieldtype !== 'keyword' && $fieldtype !== 'text') {
            $this->throwWindException($field . ' 字段数量类型为' . $fieldtype . ',无法进行字符串匹配。（字符串匹配过滤只适用于keyword、text数据类型）', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未配置索引', 0);
        }
        $isKeyWordField = false;
        if ($this->isKeyWordField($field)) {
            $isKeyWordField = true;
        }

        foreach ($suffix_terms as $suffix_term) {
            if (mb_strlen($suffix_term) > 2) {
                $sub = mb_substr($suffix_term, -3);
            } else if (mb_strlen($suffix_term) > 1) {
                $sub = mb_substr($suffix_term, -2);
            } else if (mb_strlen($suffix_term) > 0) {
                $sub = mb_substr($suffix_term, -1);
            } else {
                $sub = '';
            }
            $dir = $this->indexDir . $this->IndexName . '/index/_suffix_index.db';
            $pdo_suffix_index = new PDO_sqlite($dir);
            $sql = "select ids from $field where term='$sub';";
            $resRow = $pdo_suffix_index->getRow($sql);
            $termArr = [];
            if ($resRow) {
                $termlist = (array)json_decode($resRow['ids'], true);
                foreach ($termlist as $term) {
                    if ($this->isEndingOfString($suffix_term, $term)) {
                        if (!$is_contain_perfect_match) {
                            if ($suffix_term !== $term) {
                                $termArr[] = $term;
                            }
                        } else {
                            $termArr[] = $term;
                        }
                    }
                }
            }
            if (!empty($termArr)) {
                $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
                $pdo = new PDO_sqlite($dir);
                $terms_str = "'" . implode("','", $termArr) . "'";
                $sql = "select ids from $field where term in($terms_str);";
                $resAll = $pdo->getAll($sql);
                if ($resAll) {
                    foreach ($resAll as $resRow) {
                        if ($this->primarykeyType == 'Int_Incremental') {
                            if ($isKeyWordField) {
                                $ids_gzinf = $this->systemDecompression($resRow['ids']);
                                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                                $countContainer[] =    $ids_gzinf;
                            } else {
                                $resRow['ids'] = $this->systemDecompression($resRow['ids']);
                                $line = explode('/', $resRow['ids']);
                                $line[0] = $this->differentialDecompression($line[0]);
                                $shang = explode(',', $line[0]);
                                $ids = explode(',', $line[1]);
                                $bitmap = array_combine($shang, $ids);
                                $temp = [];
                                foreach ($bitmap as $s => $i) {
                                    $decimalArr = $this->bitmapInverse($s, $i);
                                    foreach ($decimalArr as $e) {
                                        $temp[] = $e;
                                    }
                                }
                                if (!empty($temp)) {
                                    $countContainer[] = implode(',', $temp);
                                }
                            }
                        } else {
                            $ids_gzinf = $this->systemDecompression($resRow['ids']);
                            $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                            $countContainer[] =    $ids_gzinf;
                        }
                    }
                }
            }
            $termArrTemp = [];
            $idsArrTemp = [];
            $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
            if (is_file($dir . '/terms')) {
                $terms = json_decode(file_get_contents($dir . '/terms'), true);
                $terms = (array)$terms[1];
                $firstLetter = $this->getFirstLetter(mb_substr($suffix_term, -1));
                $currFirstLetterTerms = isset($terms[$firstLetter]) ? $terms[$firstLetter] : [];
                foreach ($currFirstLetterTerms as $t) {
                    if ($this->isEndingOfString($suffix_term, $t)) {
                        $termArrTemp[] = $t;
                    }
                }
                if (!empty($termArrTemp)) {
                    foreach ($termArrTemp as $t) {
                        $realTimeData = $this->getRealTimeData($field, $t);
                        if ($realTimeData !== false) {
                            $idsArrTemp[] = $realTimeData;
                        }
                    }
                }
            }
            if (!empty($idsArrTemp)) {
                $countContainer[] = implode(',', $idsArrTemp);
            }
        }
        if (!empty($countContainer)) {
            $countContainer = explode(',', implode(',', $countContainer));
            if ($is_return_all) {
                return $countContainer;
            } else {
                return array_unique($countContainer);
            }
        } else {
            return [];
        }
    }

    private function match_prefix_suffix($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $field = $queryList['field'];
        $text = $queryList['text'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $filterIds = $this->getKeywordIndexDataEntry($queryList);
        $match_prefix_list = [];
        $match_suffix_list = [];
        $match_complete_list = [];
        if (is_string($text)) {
            $terms_list = [$text];
        } else if (is_array($text)) {
            $terms_list = $text;
        } else {
            $terms_list = [];
        }
        foreach ($terms_list as $t) {
            if (mb_substr($t, -1) == '*') {
                $match_prefix_list[] = mb_substr($t, 0, -1);
            } else if (mb_substr($t, 0, 1) == '*') {
                $match_suffix_list[] = mb_substr($t, 1);
            } else {
                $match_complete_list[] = $t;
            }
        }

        $match_res = [];
        if (!empty($match_prefix_list)) {
            $keywordIndexData = $this->filter_prefix($field, $match_prefix_list, false, false);
            if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                $match_res[] = $keywordIndexData;
            }
        }
        if (!empty($match_suffix_list)) {
            $keywordIndexData = $this->filter_suffix($field, $match_suffix_list, false, false);
            if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                $match_res[] = $keywordIndexData;
            }
        }
        if (!empty($match_complete_list)) {
            $keywordIndexData = $this->getKeywordAndTextIndexData($field, $match_complete_list, true);
            if (is_array($keywordIndexData) && !empty($keywordIndexData)) {
                $match_res[] = $keywordIndexData;
            }
        }
        $res = [];
        if (!empty($match_res)) {
            $res = (array)array_count_values(array_merge(...$match_res));
            if ($queryList['match'] && !empty($queryList['match'])) {
                $intersection = $this->multi_skip_intersection(array_keys($res), $filterIds);
                if (!empty($intersection)) {
                    $temp = [];
                    foreach ($intersection as $k) {
                        $temp[$k] = $res[$k];
                    }
                    $res = $temp;
                }
            }
        }
        arsort($res);
        $resArrScore = $res;
        $resArrScore = array_slice($resArrScore, 0, $this->searchResMinNum, true);
        $ids_all_score = $resArrScore;
        $id_arr = array_keys($resArrScore);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );
        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function match_suffix($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $field = $queryList['field'];
        $text = $queryList['text'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $countContainer = [];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未被索引', 0);
        }
        $filterIds = $this->getKeywordIndexDataEntry($queryList);
        $isKeyWordField = false;
        if ($this->isKeyWordField($queryList['field'])) {
            $isKeyWordField = true;
        }
        if (mb_strlen($text) > 2) {
            $sub = mb_substr($text, -3);
        } else if (mb_strlen($text) > 1) {
            $sub = mb_substr($text, -2);
        } else if (mb_strlen($text) > 0) {
            $sub = mb_substr($text, -1);
        } else {
            $sub = '';
        }
        $dir = $this->indexDir . $this->IndexName . '/index/_suffix_index.db';
        $pdo_suffix_index = new PDO_sqlite($dir);
        $sql = "select ids from $field where term='$sub';";
        $resRow = $pdo_suffix_index->getRow($sql);
        $termArr = [];
        if ($resRow) {
            $termlist = (array)json_decode($resRow['ids'], true);
            foreach ($termlist as $term) {
                if ($this->isEndingOfString($text, $term)) {
                    $termArr[] = $term;
                }
            }
        }
        if (!empty($termArr)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
            $pdo = new PDO_sqlite($dir);
            foreach ($termArr as $t) {
                $sql = "select ids from $field where term='$t';";
                $resRow = $pdo->getRow($sql);
                if ($resRow) {
                    if ($this->primarykeyType == 'Int_Incremental') {
                        if ($isKeyWordField) {
                            $ids_gzinf = $this->systemDecompression($resRow['ids']);
                            $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                            $countContainer[] =    $ids_gzinf;
                        } else {
                            $resRow['ids'] = $this->systemDecompression($resRow['ids']);
                            $line = explode('/', $resRow['ids']);
                            $line[0] = $this->differentialDecompression($line[0]);
                            $shang = explode(',', $line[0]);
                            $ids = explode(',', $line[1]);
                            $bitmap = array_combine($shang, $ids);
                            foreach ($bitmap as $s => $i) {
                                $decimalArr = $this->bitmapInverse($s, $i);
                                foreach ($decimalArr as $e) {
                                    $countContainer[] = $e;
                                }
                            }
                        }
                    } else {
                        $ids_gzinf = $this->systemDecompression($resRow['ids']);
                        $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                        $countContainer[] =    $ids_gzinf;
                    }
                }
            }
        }
        $termArrTemp = [];
        $idsArrTemp = [];
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        if (is_file($dir . '/terms')) {
            $terms = json_decode(file_get_contents($dir . '/terms'), true);
            $terms = (array)$terms[1];
            $firstLetter = $this->getFirstLetter(mb_substr($text, -1));
            $currFirstLetterTerms = isset($terms[$firstLetter]) ? $terms[$firstLetter] : [];
            foreach ($currFirstLetterTerms as $t) {
                if ($this->isEndingOfString($text, $t)) {
                    $termArrTemp[] = $t;
                }
            }
            if (!empty($termArrTemp)) {
                foreach ($termArrTemp as $t) {
                    $realTimeData = $this->getRealTimeData($field, $t);
                    if ($realTimeData !== false) {
                        $idsArrTemp[] = $realTimeData;
                    }
                }
            }
        }
        if (!empty($idsArrTemp)) {
            $countContainer[] = implode(',', $idsArrTemp);
        }


















        $countContainer = explode(',', implode(',', $countContainer));
        if ($queryList['match'] && !empty($queryList['match'])) {
            $countContainer = $this->multi_skip_intersection($countContainer, $filterIds);
        }

        $resArrScore = array_count_values($countContainer);
        $resArrScore = array_slice($resArrScore, 0, $this->searchResMinNum, true);
        $ids_all_score = $resArrScore;
        $id_arr = array_keys($resArrScore);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );
        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function match_prefix($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $field = $queryList['field'];
        $text = $queryList['text'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $countContainer = [];
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isIndexField($field)) {
            $this->throwWindException($field . ' 字段未被索引', 0);
        }
        $filterIds = $this->getKeywordIndexDataEntry($queryList);
        $isKeyWordField = false;
        if ($this->isKeyWordField($queryList['field'])) {
            $isKeyWordField = true;
        }
        if (mb_strlen($text) > 2) {
            $sub = mb_substr($text, 0, 3);
        } else if (mb_strlen($text) > 1) {
            $sub = mb_substr($text, 0, 2);
        } else if (mb_strlen($text) > 0) {
            $sub = mb_substr($text, 0, 1);
        } else {
            $sub = '';
        }
        $dir = $this->indexDir . $this->IndexName . '/index/_prefix_index.db';
        $pdo_prefix_index = new PDO_sqlite($dir);
        $sql = "select ids from $field where term='$sub';";
        $resRow = $pdo_prefix_index->getRow($sql);
        $termArr = [];
        if ($resRow) {
            $termlist = (array)json_decode($resRow['ids'], true);
            foreach ($termlist as $term) {
                if ($this->isBeginningOfString($text, $term)) {
                    $termArr[] = $term;
                }
            }
        }
        if (!empty($termArr)) {
            $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
            $pdo = new PDO_sqlite($dir);
            foreach ($termArr as $t) {
                $sql = "select ids from $field where term='$t';";
                $resRow = $pdo->getRow($sql);
                if ($resRow) {
                    if ($this->primarykeyType == 'Int_Incremental') {
                        if ($isKeyWordField) {
                            $ids_gzinf = $this->systemDecompression($resRow['ids']);
                            $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                            $countContainer[] =    $ids_gzinf;
                        } else {
                            $resRow['ids'] = $this->systemDecompression($resRow['ids']);
                            $line = explode('/', $resRow['ids']);
                            $line[0] = $this->differentialDecompression($line[0]);
                            $shang = explode(',', $line[0]);
                            $ids = explode(',', $line[1]);
                            $bitmap = array_combine($shang, $ids);
                            foreach ($bitmap as $s => $i) {
                                $decimalArr = $this->bitmapInverse($s, $i);
                                foreach ($decimalArr as $e) {
                                    $countContainer[] = $e;
                                }
                            }
                        }
                    } else {
                        $ids_gzinf = $this->systemDecompression($resRow['ids']);
                        $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                        $countContainer[] =    $ids_gzinf;
                    }
                }
            }
        }
        $termArrTemp = [];
        $idsArrTemp = [];
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        if (is_file($dir . '/terms')) {
            $terms = json_decode(file_get_contents($dir . '/terms'), true);
            $terms = (array)$terms[0];
            $firstLetter = $this->getFirstLetter($text);
            $currFirstLetterTerms = isset($terms[$firstLetter]) ? $terms[$firstLetter] : [];
            foreach ($currFirstLetterTerms as $t) {
                if ($this->isBeginningOfString($text, $t)) {
                    $termArrTemp[] = $t;
                }
            }
            if (!empty($termArrTemp)) {
                foreach ($termArrTemp as $t) {
                    $realTimeData = $this->getRealTimeData($field, $t);
                    if ($realTimeData !== false) {
                        $idsArrTemp[] = $realTimeData;
                    }
                }
            }
        }
        if (!empty($idsArrTemp)) {
            $countContainer[] = implode(',', $idsArrTemp);
        }
        $countContainer = explode(',', implode(',', $countContainer));
        if ($queryList['match'] && !empty($queryList['match'])) {
            $countContainer = $this->multi_skip_intersection($countContainer, $filterIds);
        }

        $resArrScore = array_count_values($countContainer);
        $resArrScore = array_slice($resArrScore, 0, $this->searchResMinNum, true);
        $ids_all_score = $resArrScore;
        $id_arr = array_keys($resArrScore);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );
        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }
    private function match_range($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $countContainer = (array)$this->getFilterIds($queryList);

        $countContainer = array_count_values(array_values($countContainer));
        $resArrScore = array_slice($countContainer, 0, $this->searchResMinNum, true);
        $ids_all_score = $resArrScore;
        $id_arr = array_keys($resArrScore);
        $ids_all = implode(',', $id_arr);
        $total = count($id_arr);
        $info['total'] = count($id_arr);
        $id_score = [];
        $f_arr = [];
        if (!empty($id_arr)) {
            $qs = ($page - 1) * $listRows;
            $js = ($page - 1) * $listRows + $listRows;
            for ($i = $qs; $i < $js; ++$i) {
                if (!isset($id_arr[$i])) {
                    continue;
                }
                array_push($f_arr, $id_arr[$i]);
                $id_score[$id_arr[$i]] = $ids_all_score[$id_arr[$i]];
            }
        }
        if (count($f_arr) > 1) {
            $id_str = implode(',', $f_arr);
        } else {
            $id_str = isset($f_arr[0]) ? $f_arr[0] : '';
        }
        $intersection = array(
            'id_str' => $id_str,             'all_id_str' => $ids_all,             'id_score' => $id_score,             'all_id_score' => $ids_all_score,             'curr_listRows_real' => count($f_arr),             'total' => $total,
        );
        $result_info = array(
            'intersection' => $intersection,
            'info' => $info,
        );
        $this->cache($cacheKey, $this->build_cache_data($result_info));
        return $result_info;
    }

    private function match_all($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $order = $queryList['order'];
        $page = $queryList['page'];
        $listRows = $queryList['list_rows'];
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        $pdo = new PDO_sqlite($dir);
        $num = $listRows;
        $resList = [];
        $min_max_id = $this->get_minid_maxid($pdo);
        if ($min_max_id) {
            $minId = $min_max_id[0];
            $maxid = $min_max_id[1];
            $docNum = $min_max_id[2];
            if ($order == 'asc') {
                $b1 = $minId + ($page - 1) * $num;
                $b2 = $minId + $page * $num - 1;
            } else {
                $b1 = $maxid - $page * $num + 1;
                $b2 = $maxid - ($page - 1) * $num;
            }
            $ids = implode(',', range($b1, $b2));
            $sql = "select $this->primarykey from $this->IndexName where $this->sys_primarykey between $b1 and $b2;";

            $resAll = $pdo->getAll($sql);
            if ($resAll) {
                $resList = $resAll;
            }
        }


        $res = [
            'ids' => implode(',', array_column($resList, $this->primarykey)),
            'info' => [
                'total' => $docNum,
            ],
        ];
        $this->cache($cacheKey, json_encode($res));
        return $res;
    }

    private function match_primarykey($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $primarykey = $queryList['primarykey'];
        if (empty($primarykey) || ($primarykey == '')) {
            $res = [
                'ids' => '',
                'info' => [
                    'total' => 0,
                ],
                'id_score' => [],
            ];
        } else {
            $score = [];
            if (is_array($primarykey)) {
                $primarykey = array_filter($primarykey);

                $idStrLen = count($primarykey);
                $idStr = implode(',', $primarykey);
                $id_score = array_count_values($primarykey);
            } else {
                $primarykeyStr = (string)$primarykey;
                $primarykey = array_filter(explode(',', $primarykeyStr));

                $idStrLen = count($primarykey);
                $idStr = implode(',', $primarykey);
                $id_score = array_count_values($primarykey);
            }
            $res = [
                'ids' => $idStr,
                'info' => [
                    'total' => $idStrLen,
                ],
                'id_score' => $id_score,
            ];
        }
        $this->cache($cacheKey, json_encode($res));
        return $res;
    }

    private function match_rand($queryList)
    {
        $cacheKey = $this->get_cachekey($queryList);
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $num = $queryList['size'];
        if ($num == 0) {
            $res = [
                'ids' => '',
                'info' => [
                    'total' => 0,
                ],
                'id_score' => [],
            ];
        } else {
            $dir = $this->getStorageDir();
            $dir = $dir . 'baseData/' . $this->IndexName . '.db';
            $pdo = new PDO_sqlite($dir);
            $maxMinId  = $this->get_minid_maxid($pdo);
            if ($maxMinId) {
                $docNum = $maxMinId[2];
                if ($docNum <= $num) {
                    $rand_primarykey = range(1, $docNum);
                } else if ($docNum <= 50) {
                    $build_primarykey = range(1, $docNum);
                    shuffle($build_primarykey);
                    $rand_primarykey = array_slice($build_primarykey, 0, $num);
                } else {
                    $rand_primarykey = [];
                    while (count($rand_primarykey) < $num) {
                        $rand_primarykey[] = mt_rand(1, $docNum);
                        $rand_primarykey = array_unique($rand_primarykey);
                    }
                }
            } else {
                $rand_primarykey = [];
            }
            $score = [];
            if (!empty($rand_primarykey)) {
                $sys_primarykey = implode(',', $rand_primarykey);
                $sql = "select $this->primarykey from $this->IndexName where $this->sys_primarykey in($sys_primarykey);";
                $resAll = $pdo->getAll($sql);
                if ($resAll) {
                    $rand_primarykey = array_column($resAll, $this->primarykey);
                } else {
                    $rand_primarykey = [];
                }
                $idStrLen = count($rand_primarykey);
                $idStr = implode(',', $rand_primarykey);

                $id_score = array_count_values($rand_primarykey);
            } else {
                $idStr = '';
                $idStrLen = 0;
                $id_score = [];
            }
            $res = [
                'ids' => $idStr,
                'info' => [
                    'total' => $idStrLen,
                ],
                'id_score' => $id_score,
            ];
        }
        $this->cache($cacheKey, $this->build_cache_data($res));
        return $res;
    }

    private function get_cachekey($queryList)
    {
        if (!isset($queryList['sort']) || empty($queryList['sort']) || isset($queryList['sort']['_score'])) {
            if ($queryList['mode'] !== 'match_phrase') {
                $cacheKey = md5(json_encode($queryList));
                return $cacheKey;
            } else {
                $queryList['page'] = 1;
                $queryList['list_rows'] = 10;
                $cacheKey = md5(json_encode($queryList));
                return $cacheKey;
            }
        } else {
            $queryList['page'] = 1;
            $queryList['list_rows'] = 10;
            $cacheKey = md5(json_encode($queryList));
            return $cacheKey;
        }
    }

    private function build_cache_data($res)
    {
        $queryList = $this->queryList;

        if (!isset($queryList['sort']) || empty($queryList['sort']) || isset($queryList['sort']['_score'])) {
            if ($queryList['mode'] !== 'match_phrase') {
                if (isset($res['intersection']['all_id_str'])) {
                    $res['intersection']['all_id_str'] = '';
                }
                if (isset($res['intersection']['all_id_score'])) {
                    $res['intersection']['all_id_score'] = '';
                }
            }

            return json_encode($res);
        } else {
            return json_encode($res);
        }
    }

    private function searchEntry($queryList)
    {
        $this->queryList = $queryList;
        $mode = $queryList['mode'];
        if ($mode == 'match' || $mode == 'match_terms' || $mode == 'match_phrase' || $mode == 'match_prefix' || $mode == 'match_suffix' || $mode == 'match_prefix_suffix' || $mode == 'match_fuzzy') {
            if ($mode == 'match_prefix') {
                return $this->match_prefix($queryList);
            } else if ($mode == 'match_suffix') {
                return $this->match_suffix($queryList);
            } else if ($mode == 'match_prefix_suffix') {
                return $this->match_prefix_suffix($queryList);
            } else if ($mode == 'match_fuzzy') {
                return $this->match_fuzzy($queryList);
            } else {
                if ($this->primarykeyType == 'Int_Incremental') {
                    $isKeyWordField = $this->isKeyWordField($queryList['field']);
                    if ($isKeyWordField) {
                        return $this->postingListMatch($queryList);
                    } else {
                        if (isset($queryList['filter']['conditions']) || isset($queryList['filter']['range']) || (isset($queryList['sort']) && !empty($queryList['sort']) && !isset($queryList['sort']['_score']))) {
                            if (isset($queryList['fc_arr']) && count($queryList['fc_arr']) > 1) {
                                $queryList['minimum_should_match'] = (isset($queryList['minimum_should_match']) && $queryList['minimum_should_match'] !== false) ? $queryList['minimum_should_match'] : 2;
                            }

                            return $this->postingListMatch($queryList, '_postinglist');
                        } else {

                            return $this->match($queryList);
                        }
                    }
                } else {
                    if (isset($queryList['filter']['conditions']) || isset($queryList['filter']['range']) || (isset($queryList['sort']) && !empty($queryList['sort']) && !isset($queryList['sort']['_score']))) {
                        if (isset($queryList['fc_arr']) && count($queryList['fc_arr']) > 1) {
                            $queryList['minimum_should_match'] = (isset($queryList['minimum_should_match']) && $queryList['minimum_should_match'] !== false) ? $queryList['minimum_should_match'] : 2;
                        }
                    }
                    return $this->postingListMatch($queryList);
                }
            }
        } else if ($mode == 'match_range') {
            return $this->match_range($queryList);
        } else if ($mode == 'multi_match') {
            return $this->multiMatchEntry($queryList);
        } else if ($mode == 'match_all') {
            return $this->match_all($queryList);
        } else if ($mode == 'match_primarykey') {
            return $this->match_primarykey($queryList);
        } else if ($mode == 'match_rand') {
            return $this->match_rand($queryList);
        } else if ($mode == 'match_bool') {
            return $this->matchBoolEntry($queryList);
        } else {
            return [];
        }
    }
    private $queryList = [];

    private function postingListSynonymMergeOneDimensional($synonym_mapping, $resultContainerOfAllTerms, $id_arr)
    {
        $tycResult = [];
        $temp = [];
        if (!empty($synonym_mapping)) {
            foreach ($synonym_mapping as $v => $c) {
                if (is_array($c) && !empty($c)) {
                    array_unshift($c, $v);
                    $arr_mer = [];
                    foreach ($c as $z) {
                        if (!isset($resultContainerOfAllTerms[$z])) {
                            continue;
                        }
                        $temp = $resultContainerOfAllTerms[$z];
                        if (is_array($temp)) {
                            $arr_mer = array_merge($arr_mer, $temp);
                        }
                    }
                    $arr_mer = array_unique($arr_mer);
                    $tycResult[$v] = $arr_mer;
                } else {
                    $tycResult[$v] = $resultContainerOfAllTerms[$v];
                }
            }
            unset($resultContainerOfAllTerms);
            $tycResultTemp = [];
            foreach ($tycResult as $v => $c) {
                if (is_array($c)) {
                    $tycResultTemp[$v] = implode(',', $c);
                }
            }
            $id_arr = array_values($tycResultTemp);
            $arr = array(
                'id_arr' => $id_arr,                 'tycResult' => $tycResult,
            );
            return $arr;
        } else {
            $arr = array(
                'id_arr' => $id_arr,                 'tycResult' => $resultContainerOfAllTerms,
            );
            return $arr;
        }
    }

    private function postingListSynonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $id_arr)
    {
        $tycResult = [];
        $temp = [];
        if (!empty($synonym_mapping)) {
            foreach ($synonym_mapping as $v => $c) {
                if (is_array($c) && !empty($c)) {
                    array_unshift($c, $v);
                    $arr_mer = [];
                    foreach ($c as $z) {
                        if (is_array($z)) {
                            $intersectTemp = [];
                            foreach ($z as $k) {
                                if (!isset($resultContainerOfAllTerms[$k])) {
                                    $intersectTemp = [];
                                    break;
                                }
                                if (empty($intersectTemp)) {
                                    $intersectTemp = $resultContainerOfAllTerms[$k];
                                } else {
                                    $intersectTemp = $this->multi_skip_intersection($intersectTemp, $resultContainerOfAllTerms[$k]);
                                }
                            }
                            $temp = $intersectTemp;
                        } else {
                            if (!isset($resultContainerOfAllTerms[$z])) {
                                continue;
                            }
                            $temp = $resultContainerOfAllTerms[$z];
                        }
                        if (is_array($temp)) {
                            $arr_mer = array_merge($arr_mer, $temp);
                        }
                    }
                    $arr_mer = array_unique($arr_mer);
                    $tycResult[$v] = $arr_mer;
                } else {
                    $tycResult[$v] = $resultContainerOfAllTerms[$v];
                }
            }
            unset($resultContainerOfAllTerms);
            $tycResultTemp = [];
            foreach ($tycResult as $v => $c) {
                if (is_array($c)) {
                    $tycResultTemp[$v] = implode(',', $c);
                }
            }
            $id_arr = array_values($tycResultTemp);
            $arr = array(
                'id_arr' => $id_arr,                 'tycResult' => $tycResult,
            );
            return $arr;
        } else {
            $arr = array(
                'id_arr' => $id_arr,                 'tycResult' => $resultContainerOfAllTerms,
            );
            return $arr;
        }
    }

    private function synonymMergeTwoDimensional($synonym_mapping, $resultContainerOfAllTerms, $bitmapArr)
    {
        if (!empty($synonym_mapping)) {
            $bitmapArrMergeSimilar = [];
            foreach ($synonym_mapping as $v => $c) {
                if (!isset($resultContainerOfAllTerms[$v])) {
                    continue;
                }
                if (is_array($c) && !empty($c)) {

                    $bitmapArrMerge = [];
                    $bitmapArrMerge2 = [];
                    foreach ($c as $z) {
                        if (is_array($z)) {
                            foreach ($z as $t) {
                                if (!isset($resultContainerOfAllTerms[$t])) {
                                    $bitmapArrMerge2 = [];
                                    break;
                                }
                                if (empty($bitmapArrMerge2)) {
                                    $bitmapArrMerge2 = $resultContainerOfAllTerms[$t];
                                } else {
                                    $bitmapArrMerge2 = array_merge_recursive($bitmapArrMerge2, $resultContainerOfAllTerms[$t]);
                                }
                            }
                            if (!empty($bitmapArrMerge2)) {
                                $bitmapArrMergeCount2 = [];
                                $z_len = count($z);
                                foreach ($bitmapArrMerge2 as $k => $a) {
                                    if (is_array($a) && (count($a) == $z_len)) {
                                        foreach ($a as $d) {
                                            if (!isset($bitmapArrMergeCount2[$k])) {
                                                $bitmapArrMergeCount2[$k] = $d;
                                            } else {
                                                $bitmapArrMergeCount2[$k] = $bitmapArrMergeCount2[$k] & (int)$d;
                                            }
                                        }
                                    }
                                }

                                if (empty($bitmapArrMergeCount2)) {
                                    continue;
                                } else {
                                    $bitmapArrMergeTemp = $bitmapArrMergeCount2;
                                }
                            } else {
                                continue;
                            }
                        } else {
                            if (!isset($resultContainerOfAllTerms[$z])) {
                                continue;
                            }
                            $bitmapArrMergeTemp = $resultContainerOfAllTerms[$z];
                        }
                        if (empty($bitmapArrMerge)) {
                            $bitmapArrMerge = $bitmapArrMergeTemp;
                        } else {
                            $bitmapArrMerge = $this->array_merge_recursive($bitmapArrMerge, $bitmapArrMergeTemp);
                        }
                    }
                    if (!empty($bitmapArrMerge)) {
                        $bitmapArrMergeCount = [];
                        $c_len = count($c);
                        if ($c_len == 1) {
                            $bitmapArrMergeCount = $bitmapArrMerge;
                        } else {
                            foreach ($bitmapArrMerge as $k => $a) {
                                if (is_array($a)) {
                                    foreach ($a as $d) {
                                        if (!isset($bitmapArrMergeCount[$k])) {
                                            $bitmapArrMergeCount[$k] = $d;
                                        } else {
                                            $bitmapArrMergeCount[$k] = $bitmapArrMergeCount[$k] | (int)$d;
                                        }
                                    }
                                } else {
                                    $bitmapArrMergeCount[$k] = (int)$a;
                                }
                            }
                        }
                        foreach ($resultContainerOfAllTerms[$v] as $k => $e) {
                            if (!isset($bitmapArrMergeCount[$k])) {
                                $bitmapArrMergeCount[$k] = $e;
                            } else {
                                $bitmapArrMergeCount[$k] = $bitmapArrMergeCount[$k] | (int)$e;
                            }
                        }
                    } else {
                        $bitmapArrMergeCount = $resultContainerOfAllTerms[$v];
                    }
                    $bitmapArrMergeSimilar[] = $bitmapArrMergeCount;
                } else {
                    $bitmapArrMergeSimilar[] = $resultContainerOfAllTerms[$v];
                }
            }
            return $bitmapArrMergeSimilar;
        } else {
            return $bitmapArr;
        }
    }

    private function synonymMergeOneDimensional($synonym_mapping, $resultContainerOfAllTerms, $bitmapArr)
    {
        if (!empty($synonym_mapping)) {
            $bitmapArrMergeSimilar = [];
            foreach ($synonym_mapping as $v => $c) {
                if (!isset($resultContainerOfAllTerms[$v])) {
                    continue;
                }
                if (is_array($c) && !empty($c)) {

                    array_unshift($c, $v);

                    $bitmapArrMerge = [];
                    foreach ($c as $z) {
                        if (!isset($resultContainerOfAllTerms[$z])) {
                            continue;
                        }
                        $bitmapArrMergeTemp = $resultContainerOfAllTerms[$z];
                        if (empty($bitmapArrMerge)) {
                            $bitmapArrMerge = $bitmapArrMergeTemp;
                        } else {
                            $bitmapArrMerge = $this->array_merge_recursive($bitmapArrMerge, $bitmapArrMergeTemp);
                        }
                    }
                    $bitmapArrMergeCount = [];
                    if (!empty($bitmapArrMerge)) {
                        foreach ($bitmapArrMerge as $k => $a) {
                            if (is_array($a)) {
                                foreach ($a as $d) {
                                    if (!isset($bitmapArrMergeCount[$k])) {
                                        $bitmapArrMergeCount[$k] = $d;
                                    } else {
                                        $bitmapArrMergeCount[$k] = $bitmapArrMergeCount[$k] | (int)$d;
                                    }
                                }
                            } else {
                                $bitmapArrMergeCount[$k] = $a;
                            }
                        }
                    } else {
                        $bitmapArrMergeCount = $resultContainerOfAllTerms[$v];
                    }
                    $bitmapArrMergeSimilar[] = $bitmapArrMergeCount;
                } else {
                    $bitmapArrMergeSimilar[] = $resultContainerOfAllTerms[$v];
                }
            }

            return $bitmapArrMergeSimilar;
        } else {
            return $bitmapArr;
        }
    }
    private $realTimeIndex = false;

    public function onRealTimeIndex()
    {
        $this->realTimeIndex = true;
    }

    public function closeRealTimeIndex()
    {
        $this->realTimeIndex = false;
    }
    private $realTimeIndexCount = 0;
    private $real_time_index_of_text_and_keyword = [];

    private function real_time_index_postlist($field, $fc_arr, $id)
    {
        if ($id === '') {
            return false;
        }
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        ++$this->realTimeIndexCount;
        foreach ($fc_arr as $q) {
            $szm = $this->getFirstLetter($q);
            if (!isset($this->real_time_index_of_text_and_keyword[$field][$szm])) {
                if (!is_dir($dir . '/' . $szm)) {
                    mkdir($dir . '/' . $szm);
                }
                $index_file = $dir . '/' . $szm . '/dp.index';
                if (!is_file($index_file)) {
                    file_put_contents($index_file, json_encode([]));
                }
                $dp = file_get_contents($index_file);
                $dp = (array)json_decode($dp, true);
                $this->real_time_index_of_text_and_keyword[$field][$szm] = $dp;
            }
            if (!isset($this->real_time_index_of_text_and_keyword[$field][$szm][$q])) {
                $this->real_time_index_of_text_and_keyword[$field][$szm][$q] = $id;
            } else {
                $this->real_time_index_of_text_and_keyword[$field][$szm][$q] = $id . ',' . $this->real_time_index_of_text_and_keyword[$field][$szm][$q];
            }
        }
    }

    private function real_time_index_bitmap($field, $fc_arr, $id)
    {
        if ($id === '') {
            return false;
        }
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        ++$this->realTimeIndexCount;
        $bitmapArr = [];
        $quotient = floor($id / 64);
        $remainder = $id % 64;
        if (!isset($bitmapArr[$quotient])) {
            $bitmapArr[$quotient] = 1 << intval($remainder);
        } else {

            $bitmapArr[$quotient] = $bitmapArr[$quotient] | (1 << intval($remainder));
        }
        foreach ($fc_arr as $q) {
            $szm = $this->getFirstLetter($q);
            if (!isset($this->real_time_index_of_text_and_keyword[$field][$szm])) {
                if (!is_dir($dir . '/' . $szm)) {
                    mkdir($dir . '/' . $szm);
                }
                $index_file = $dir . '/' . $szm . '/dp.index';
                if (!is_file($index_file)) {
                    file_put_contents($index_file, json_encode([]));
                }
                $bitmap = file_get_contents($index_file);
                $bitmap = json_decode($bitmap, true);
                $this->real_time_index_of_text_and_keyword[$field][$szm] = $bitmap;
            }
            if (!isset($this->real_time_index_of_text_and_keyword[$field][$szm][$q])) {
                $tempBitmap = [
                    $quotient => $bitmapArr[$quotient],
                ];
                $this->real_time_index_of_text_and_keyword[$field][$szm][$q] = $tempBitmap;
            } else {
                if (isset($this->real_time_index_of_text_and_keyword[$field][$szm][$q][$quotient])) {
                    $this->real_time_index_of_text_and_keyword[$field][$szm][$q][$quotient] = (int)$bitmapArr[$quotient] | (int)$this->real_time_index_of_text_and_keyword[$field][$szm][$q][$quotient];
                } else {
                    $this->real_time_index_of_text_and_keyword[$field][$szm][$q][$quotient]  = $bitmapArr[$quotient];
                }
            }
        }
    }
    private $real_time_index_of_storage_terms = [];
    private function real_time_index_storage_terms($field, $terms)
    {
        $this->real_time_index_of_storage_terms[$field][] = $terms;
    }
    private $real_time_index_of_geo_point = [];

    private function real_time_index_geo_point($field, $list)
    {
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        $index_file = $dir . '/real_time_data.index';
        if (!is_file($index_file)) {
            file_put_contents($index_file, json_encode([]));
        }
        if (!isset($this->real_time_index_of_geo_point[$field])) {
            $dp = file_get_contents($index_file);
            $dp = (array)json_decode($dp, true);
            $this->real_time_index_of_geo_point[$field] = $dp;
        }
        foreach ($list as $v) {
            $id = $v['id'];
            $radian = $v['radian'];
            if ($id) {
                $this->real_time_index_of_geo_point[$field][$id] = $radian;
            }
        }
    }
    private $real_time_index_of_numeric_and_date = [];
    private function realtimeIndexNumericAndDate()
    {
        if (!empty($this->numericContainer)) {
            foreach ($this->numericContainer as $fd => $list) {
                $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
                if (!is_dir($dir)) {
                    mkdir($dir, 0777, true);
                }
                if (!isset($this->real_time_index_of_numeric_and_date[$fd])) {
                    $this->real_time_index_of_numeric_and_date[$fd] =  (array)json_decode(file_get_contents($dir . '/dp_interval.index'), true);
                }
                $temp = [];
                $group = [];
                $res = [];
                foreach ($list as $d => $n) {
                    $interval = $this->getNumericInterval($n, $fd);
                    if (isset($temp[$interval][$n])) {
                        $temp[$interval][$n] .= ',' . $d;
                    } else {
                        $temp[$interval][$n] = $d;
                        $group[$interval][$n] = $d;
                    }
                }
                foreach ($temp as $i => $v) {
                    $res[$i] = [
                        $v,
                        $group[$i],
                    ];
                }
                if (!empty($res)) {
                    foreach ($res as $i => $v) {
                        $l = $v[0];
                        $f = $v[1];
                        foreach ($l as $num => $str) {
                            if (isset($this->real_time_index_of_numeric_and_date[$fd][$i][0][$num])) {
                                $this->real_time_index_of_numeric_and_date[$fd][$i][0][$num] .= ',' . $str;
                            } else {
                                $this->real_time_index_of_numeric_and_date[$fd][$i][0][$num] = $str;
                                $this->real_time_index_of_numeric_and_date[$fd][$i][1][$num] = $f[$num];
                            }
                        }
                    }
                }
            }
        }
        if (!empty($this->dateContainer)) {
            foreach ($this->dateContainer as $fd => $list) {
                $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
                if (!is_dir($dir)) {
                    mkdir($dir, 0777, true);
                }
                if (!isset($this->real_time_index_of_numeric_and_date[$fd])) {
                    $this->real_time_index_of_numeric_and_date[$fd] =  (array)json_decode(file_get_contents($dir . '/dp_interval.index'), true);
                }
                $temp = [];
                $baseInterval = [];
                foreach ($list as $d => $n) {
                    $interval = $this->getDateNYR(strtotime($n));
                    $temp[$interval][] = $d;
                    $baseInterval[] = $interval;
                }
                $interval_mapping = (array)json_decode(file_get_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $fd . '_interval_mapping.txt'), true);
                $interval_mapping = array_unique(array_merge($interval_mapping, $baseInterval));
                sort($interval_mapping);
                file_put_contents($this->indexDir . $this->IndexName . '/index/interval_mapping/' . $fd . '_interval_mapping.txt', json_encode($interval_mapping));
                if (!empty($temp)) {
                    foreach ($temp as $k => $v) {
                        if (isset($this->real_time_index_of_numeric_and_date[$fd][$k])) {
                            $this->real_time_index_of_numeric_and_date[$fd][$k] = array_merge($this->real_time_index_of_numeric_and_date[$fd][$k], $v);
                        } else {
                            $this->real_time_index_of_numeric_and_date[$fd][$k] = $v;
                        }
                    }
                }
            }
        }
    }

    private function realTimeIndexBatchWrite()
    {
        $this->realTimeIndexCount;
        $dir1  = $this->indexDir . $this->IndexName . '/index/real_time_index/';
        if (is_file($dir1 . '/real_time_index_count')) {
            $real_time_index_count = (int)file_get_contents($dir1 . '/real_time_index_count');
            $real_time_index_count = $this->realTimeIndexCount + $real_time_index_count;
            file_put_contents($dir1 . '/real_time_index_count', $real_time_index_count);
        } else {
            file_put_contents($dir1 . '/real_time_index_count', 0);
        }

        foreach ($this->real_time_index_of_text_and_keyword as $fd => $map) {
            $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
            foreach ($map as $k => $v) {
                file_put_contents($dir . '/' . $k . '/dp.index', json_encode($v));
            }
        }
        $this->realTimeIndexCount = 0;
        $this->real_time_index_of_text_and_keyword = [];
        if (!empty($this->real_time_index_of_geo_point)) {
            foreach ($this->real_time_index_of_geo_point as $fd => $map) {
                $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
                $geoIndex = $dir . '/real_time_data.index';
                file_put_contents($geoIndex, json_encode($map));
            }
            $this->real_time_index_of_geo_point = [];
        }
        if (!empty($this->real_time_index_of_numeric_and_date)) {
            foreach ($this->real_time_index_of_numeric_and_date as $fd => $v) {
                $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
                file_put_contents($dir . '/dp_interval.index', json_encode($v));
            }
        }
        if (!empty($this->real_time_index_of_storage_terms)) {
            foreach ($this->real_time_index_of_storage_terms as $fd => $map) {
                $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $fd . '/block';
                if (is_file($dir . '/terms')) {
                    $old_terms = (array)json_decode(file_get_contents($dir . '/terms'), true);
                } else {
                    $old_terms = [];
                }
                $terms = array_merge(...$this->real_time_index_of_storage_terms[$fd]);
                $terms = array_unique($terms);
                foreach ($terms as $t) {
                    $firstLetter = $this->getFirstLetter($t);
                    if (isset($old_terms[0][$firstLetter])) {
                        $old_terms[0][$firstLetter][] = $t;
                    } else {
                        $old_terms[0][$firstLetter][] = $t;
                    }
                }
                foreach ($terms as $t) {
                    $lastChar = mb_substr($t, -1);
                    $firstLetter = $this->getFirstLetter($lastChar);
                    if (isset($old_terms[1][$firstLetter])) {
                        $old_terms[1][$firstLetter][] = $t;
                    } else {
                        $old_terms[1][$firstLetter][] = $t;
                    }
                }
                foreach ($old_terms as $k => $v) {
                    foreach ($v as $s => $list) {
                        $old_terms[$k][$s] = array_unique($list);
                    }
                }
                file_put_contents($dir . '/terms', json_encode($old_terms));
            }
        }
    }

    private function delRealTimeIndex()
    {
        $dp_index_dir = $this->indexDir . $this->IndexName . '/index/real_time_index';
        $this->empty_dir($dp_index_dir);
    }

    private function calculate_the_degree_of_term_aggregation($res = [], $fields = [], $terms = [])
    {
        if (is_string($fields)) {
            $fields = [
                ['name' => $fields,]
            ];
        }
        if (!empty($fields)) {
            foreach ($res as $k => $v) {
                $_score = [];
                foreach ($fields as $f) {
                    $fd = $f['name'];
                    if (!isset($v[$fd])) {
                        continue;
                    }
                    $str = $v[$fd];
                    $count = [];
                    $termsLen = count($terms) + 1;
                    foreach ($terms as $t) {
                        $loc = (int)stripos($str, $t);
                        $count[] = $loc;
                    }
                    $max = (int)max($count);
                    $min = (int)min($count);
                    $diff = ($max - $min) + 1;
                    $degree = (1 / ($diff / ($termsLen))) / 10;
                    $_score[] = $degree;
                }
                if (isset($res[$k]['_score'])) {
                    $degree_avg = array_sum($_score) / $termsLen;
                    $res[$k]['_score'] = ($res[$k]['_score'] + $degree_avg);
                }
            }
        }
        return $res;
    }

    private function normalization($x, $max, $min)
    {
        if ($max < 0 || $max == 0) {
            return 0;
        }
        if ($x > $max) {
            return 1;
        }
        $y = ($x - $min) / ($max - $min);
        return $y;
    }

    public function getCurrNum()
    {
        $dir = $this->indexDir . $this->IndexName . '/currDocNum';
        if (is_file($dir)) {
            $docNum = file_get_contents($dir);
            $docNum = (int)$docNum;
            return $docNum;
        } else {
            return 0;
        }
    }

    private function _getSynonym($terms = [])
    {
        $dir = dirname(__FILE__) . '/windIndexCore/synonym/synonymDp/_synonym.db';
        $pdo = new PDO_sqlite($dir);
        $termsStr = "'" . implode("','", $terms) . "'";
        $sql = "select * from _synonym where term in($termsStr)";
        $resAll = $pdo->getAll($sql);
        return $resAll;
    }


    private function getSynonym($terms, $query)
    {
        $res = array(
            'synonym_mapping' => [],             'synonym_merge' => [],
        );
        if (empty($terms)) {
            return $res;
        } else {


            $str_zmsz = 0;
            if (preg_match_all("/[_a-zA-Z0-9]+/i", $query, $mat_)) {
                $str_zmsz = strlen(implode('', $mat_[0]));
            };
            $ynonymArr = $this->_getSynonym($terms);
            if (!$ynonymArr) {
                return $res;
            }
            $allSynonym = [];
            $synonym_mapping = [];
            foreach ($ynonymArr as $v) {
                $term = $v['term'];
                $list = $v['ids'];
                if (preg_match("/^\w*$/", $term)) {
                    if (strlen($term) != $str_zmsz) {
                        continue;
                    }
                }
                if ($list === '') {
                    $synonym_mapping[$term] = [];
                } else {
                    $synonym_mapping[$term] = [];
                    $list_arr = explode('|', $list);
                    foreach ($list_arr as $k) {
                        if (stristr($k, ',') !== false) {
                            $k_arr = explode(',', $k);
                            $k_arr = array_filter($k_arr);
                            $allSynonym = array_merge($allSynonym, $k_arr);
                            $synonym_mapping[$term][] = array_unique($k_arr);
                        } else {
                            $allSynonym[] = $k;
                            $synonym_mapping[$term][] = $k;
                        }
                    }
                }
            }
            $synonymMap = [];
            foreach ($terms as $t) {
                if (isset($synonym_mapping[$t])) {
                    $synonymMap[$t] = $synonym_mapping[$t];
                } else {
                    $synonymMap[$t] = [];
                }
            }
            if (!empty($allSynonym)) {
                $synonymMerge = array_unique($allSynonym);
                $synonymMerge = array_filter($synonymMerge);
            } else {
                $synonymMerge = [];
            }
            $res = array(
                'synonym_mapping' => $synonymMap,                 'synonym_merge' => $synonymMerge,
            );
            return $res;
        }
    }

    public function getIndexList()
    {
        $scandir = scandir($this->indexDir);
        $arr = [];
        foreach ($scandir as $v) {
            if ($v == '.' || $v == '..') {
                continue;
            }
            $mapping = $this->indexDir . $v . '/Mapping';
            if (is_file($mapping)) {
                $mapping = json_decode(file_get_contents($mapping), true);
                $arr[$mapping['index']]['name'] = $mapping['index'];
                $arr[$mapping['index']]['mapping'] = $mapping;
                $index_info = $this->indexDir . $v . '/index_info';
                if (is_file($index_info)) {
                    $index_info = json_decode(file_get_contents($index_info), true);
                    $arr[$mapping['index']]['info'] = $index_info;
                }
            }
        }
        return $arr;
    }

    private function desensitization($res, $desens)
    {
        if (is_array($desens)) {
            foreach ($res as $k => $v) {
                foreach ($desens as $field => $cutArr) {
                    if (!isset($v[$field])) {
                        continue;
                    }
                    if (is_array($cutArr) && count($cutArr) > 0) {
                        if (isset($cutArr[0])) {
                            $cutLeft = mb_substr($res[$k][$field], 0, (int)$cutArr[0]);
                        } else {
                            $cutLeft = '';
                        }
                        if (isset($cutArr[1])) {
                            if ((int)$cutArr[1] === 0) {
                                $cutRight = '';
                            } else {
                                $cutRight = mb_substr($res[$k][$field], -(int)$cutArr[1]);
                            }
                        } else {
                            $cutRight = '';
                        }
                        $connect = isset($cutArr[2]) ? $cutArr[2] : '****';
                        $res[$k][$field] = $cutLeft . $connect . $cutRight;
                    }
                }
            }
        }
        return $res;
    }
    public function timsort($array = [])
    {
        $timsort = new TimSort();

        $timsort->sort($array);
        return $array;
    }

    private function highLight($res, $fields)
    {
        $highRes = [];
        $primarykey = $this->primarykey;
        foreach ($res as $k => $v) {
            foreach ($fields as $i => $list) {
                $field = $list['name'];
                if (!isset($res[$k][$field])) {
                    continue;
                }
                $curr_primarykey = $res[$k][$primarykey];
                $fc_arr = $list['fc_arr_original'];
                $is_cut = isset($list['highlight']['is_cut']) ? $list['highlight']['is_cut'] : '';
                $fixed_length = isset($list['highlight']['fixed_length']) ? $list['highlight']['fixed_length'] : 0;
                $fixed_length = ceil((int)$fixed_length);
                if ($fixed_length > 0) {
                    $highRes[$curr_primarykey][$field] = mb_substr($res[$k][$field], 0, $fixed_length, 'utf-8');
                }

                $ignore_characters = ['e', 'm', 'em', '<', '>', '/'];
                $terms = $fc_arr;
                $termsMapping = [];
                foreach ($terms as $v_a => $c_a) {
                    $termsMapping[$c_a] = strlen($c_a);
                }
                arsort($termsMapping);
                if ($is_cut) {
                    $highlightString = $res[$k][$field];
                    $hit_terms_position = [];
                    foreach ($termsMapping as $t => $len) {
                        if ((mb_strlen($t, 'utf-8') > 1) && !in_array($t, $ignore_characters)) {
                            $hit_terms_position[] = (int)mb_stripos($highlightString, (string)$t, 0, 'utf-8');
                        }
                    }
                    $hit_terms_position = array_unique($hit_terms_position);
                    if (count($hit_terms_position) == 0) {
                        $hit_terms_position_avg = 0;
                    } else {
                        $hit_terms_position_avg = (array_sum($hit_terms_position) / count($hit_terms_position)) - 15;
                    }
                    if ($hit_terms_position_avg - 7 < 0) {
                        $hit_terms_position_avg = 0;
                    }
                    $stringSubToEnd = mb_substr($highlightString, $hit_terms_position_avg, 500, 'utf-8');
                    if (mb_strlen($stringSubToEnd, 'utf-8') < 30) {
                        $hit_terms_position_avg -= 18;
                        if ($hit_terms_position_avg < 0) {
                            $hit_terms_position_avg = 0;
                        }
                        $highlightString = mb_substr($highlightString, $hit_terms_position_avg, 500, 'utf-8');
                    } else {
                        $highlightString = $stringSubToEnd;
                    }
                    if (mb_strlen($highlightString, 'utf-8') > 50) {
                        $highlightString = mb_substr($highlightString, 0, 40, 'utf-8') . '...';
                    }
                    if ($hit_terms_position_avg > 0) {
                        $highlightString = '...' . $highlightString;
                    }
                    $symbolEscape = array('+' => '\+', '-' => '\-', '*' => '\*', '/' => '\/', '$' => '\$', '%' => '\%', '?' => '\?', '.' => '\.', '(' => '\(', ')' => '\)', '（' => '\（', '）' => '\）', '[' => '\[', ']' => '\]', '{' => '\{', '}' => '\}', '【' => '\【', '】' => '\】', '#' => '\#');
                    $termsArr = [];
                    foreach ($termsMapping as $t => $v) {
                        $t = strtr($t, $symbolEscape);
                        $termsArr[$t] = $v;
                    }
                    $matchArr = [];
                    if (count($termsArr) > 0) {
                        $red_str = implode('|', array_keys($termsArr));
                        $reg_red = '/(' . $red_str . ')/i';
                        if (preg_match_all($reg_red, $highlightString, $mat)) {
                            $matchArr = $mat[0];
                        }
                    }
                    $matchArrMapping = [];
                    foreach ($matchArr as $v_a => $c_a) {
                        $matchArrMapping[$c_a] = mb_strlen($c_a, 'UTF-8');
                    }
                    arsort($matchArrMapping);
                    $notIgnoreSymbol = array('+', '-', '*', '=');
                    foreach ($matchArrMapping as $t => $len) {
                        $isUpper = 0;
                        $strOrd = ord($t);
                        if ($strOrd > 64 && $strOrd < 91) {
                            $isUpper = 1;
                        }
                        if (((strlen($t) >= 2) && !in_array(strtolower($t), $ignore_characters)) || (($isUpper == 1) && !in_array(strtolower($t), $ignore_characters)) || in_array($t, $notIgnoreSymbol)) {
                            $highlightString = str_ireplace($t, '<em>' . $t . '</em>', $highlightString);
                        }
                    }
                    $highRes[$curr_primarykey][$field] = $highlightString;
                } else {
                    $highlightString = $res[$k][$field];
                    foreach ($termsMapping as $t => $len) {
                        if ((strlen($t) > 0) && !in_array(strtolower($t), $ignore_characters)) {
                            $highlightString = str_ireplace($t, '<em>' . $t . '</em>', $highlightString);
                        }
                    }
                    $highRes[$curr_primarykey][$field] = $highlightString;
                }
            }
        }
        $res = [
            '_source' => $res,
            '_highlight' => $highRes,
        ];
        return $res;
    }

    private function analyzerEntry($text, $analyzer, $isAllTerm = false, $isIdf = false)
    {
        if (!is_object($this->fcHandle)) {
            $this->throwWindException('分词功能不可用', 0);
        }
        $modeArr = ['', 'not', 'complete'];
        if (in_array($analyzer, $modeArr)) {
            $fc_arr = [strtolower($text)];
        } else if ($analyzer == 'segment') {
            $fc_arr = $this->segment($text, $isAllTerm, $isIdf);
        } else if (isset($analyzer['separator'])) {
            $fc_arr = explode($analyzer['separator'], $text);
            $fc_arr = array_map(function ($t) {
                return str_replace(' ', '', strtolower($t));
            }, $fc_arr);
            $fc_arr = array_filter($fc_arr);
        } else if (isset($analyzer['ngram'])) {
            $fc_arr = $this->nGram($text, $analyzer['ngram']);
        } else {
            $fc_arr = $this->segment($text, $isAllTerm, $isIdf);
        }
        return $fc_arr;
    }

    public function maxSearchLen($len)
    {
        $this->subLen = ((int)$len < 1) ? 1 : (int)$len;
    }







    private static function scanDirectory($directory)
    {
        $files = [];
        if (!is_dir($directory)) {
            return [];
        }
        $dirHandle = opendir($directory);
        if ($dirHandle) {
            while (($entry = readdir($dirHandle)) !== false) {
                if ($entry === '.' || $entry === '..') {
                    continue;
                }
                $fullPath = $directory . DIRECTORY_SEPARATOR . $entry;
                if (is_dir($fullPath)) {
                    $files = array_merge($files, self::scanDirectory($fullPath));
                } else {
                    $files[] = $fullPath;
                }
            }
            closedir($dirHandle);
        } else {
            return [];
        }
        return $files;
    }

    public function statisticsTerms()
    {
        $dir = __DIR__ . '/statistics/' . $this->IndexName . '/storage/';
        if (!is_dir($dir)) {
            return false;
        }
        $scandir = scandir($dir);
        $yearsName = [];
        foreach ($scandir as $d) {
            if ($d == '.' || $d == '..') {
                continue;
            }
            if (is_dir($dir . $d)) {
                $yearsName[] = $d;
            }
        }
        $dirIndexName = __DIR__ . '/statistics/' . $this->IndexName . '/';
        $terms_all = [];
        $directory = $dir;
        $files = self::scanDirectory($directory);
        if (!empty($files)) {
            foreach ($files as $f) {
                if (is_file($f)) {
                    $terms_all[] = array_filter(explode(PHP_EOL, file_get_contents($f)));
                }
            }
        }
        $terms_all = array_merge(...$terms_all);
        $terms_all = array_count_values($terms_all);
        arsort($terms_all);
        $terms_all = [
            'all' => $terms_all,
        ];
        $storageDir = $dirIndexName . 'statistics/';
        if (!is_dir($storageDir)) {
            mkdir($storageDir, 0777, true);
        }
        file_put_contents($storageDir . 'all', json_encode($terms_all));
        $terms_y = [];
        foreach ($yearsName as $y) {
            $directory = $dir . $y . '/';
            $temp = [];
            $files = self::scanDirectory($directory);
            if (!empty($files)) {
                foreach ($files as $f) {
                    if (is_file($f)) {
                        $temp[] = array_filter(explode(PHP_EOL, file_get_contents($f)));
                    }
                }
            }
            $temp = array_merge(...$temp);
            $temp = array_count_values($temp);
            arsort($temp);
            $terms_y[$y] = $temp;
        }
        $storageDir = $dirIndexName . 'statistics/';
        if (!is_dir($storageDir)) {
            mkdir($storageDir, 0777, true);
        }
        file_put_contents($storageDir . 'y', json_encode($terms_y));
        $terms_m = [];
        foreach ($yearsName as $y) {
            $scandir = scandir($dir . $y . '/');
            $monthName = [];
            foreach ($scandir as $m) {
                if ($m == '.' || $m == '..') {
                    continue;
                }
                if (is_dir($dir . $y . '/' . $m . '/')) {
                    $monthName[] = $m;
                }
            }
            if (!empty($monthName)) {
                foreach ($monthName as $m) {
                    $directory = $dir . $y . '/' . $m . '/';
                    $temp = [];
                    $files = self::scanDirectory($directory);
                    if (!empty($files)) {
                        foreach ($files as $f) {
                            if (is_file($f)) {
                                $temp[] = array_filter(explode(PHP_EOL, file_get_contents($f)));
                            }
                        }
                    }
                    $temp = array_merge(...$temp);
                    $temp = array_count_values($temp);
                    arsort($temp);
                    $terms_m[$m] = $temp;
                }
            }
        }
        $storageDir = $dirIndexName . 'statistics/';
        if (!is_dir($storageDir)) {
            mkdir($storageDir, 0777, true);
        }
        file_put_contents($storageDir . 'm', json_encode($terms_m));
        $terms_d = [];
        foreach ($yearsName as $y) {
            $directory = $dir . $y . '/';
            $scandir = scandir($directory);
            $monthName = [];
            foreach ($scandir as $m) {
                if ($m == '.' || $m == '..') {
                    continue;
                }
                if (is_dir($dir . $y . '/' . $m . '/')) {
                    $monthName[] = $m;
                }
            }
            if (!empty($monthName)) {
                foreach ($monthName as $m) {
                    $directory = $dir . $y . '/' . $m . '/';
                    $scandir = scandir($directory);
                    $daysName = [];
                    foreach ($scandir as $d) {
                        if ($d == '.' || $d == '..') {
                            continue;
                        }
                        if (is_dir($dir . $y . '/' . $m . '/' . $d . '/')) {
                            $daysName[] = $d;
                        }
                    }
                    if (!empty($daysName)) {
                        foreach ($daysName as $d) {
                            $temp = [];
                            $directory = $dir . $y . '/' . $m . '/' . $d . '/';
                            $files = self::scanDirectory($directory);
                            if (!empty($files)) {
                                foreach ($files as $f) {
                                    if (is_file($f)) {
                                        $temp[] = array_filter(explode(PHP_EOL, file_get_contents($f)));
                                    }
                                }
                            }
                            $temp = array_merge(...$temp);
                            $temp = array_count_values($temp);
                            arsort($temp);
                            $terms_d[$d] = $temp;
                        }
                    }
                }
            }
        }
        $storageDir = $dirIndexName . 'statistics/';
        if (!is_dir($storageDir)) {
            mkdir($storageDir, 0777, true);
        }
        file_put_contents($storageDir . 'd', json_encode($terms_d));
    }

    public function getTermsStatistics($type = 'all')
    {
        $dir = __DIR__ . '/statistics/' . $this->IndexName . '/';
        if (!is_dir($dir)) {
            return '无索引库或没有任何搜索收集';
        }
        $storageDir = $dir . 'statistics/';
        if ($type == 'all') {
            if (!is_file($storageDir . 'all')) {
                return '无统计结果';
            } else {
                return json_decode(file_get_contents($storageDir . 'all'), true);
            }
        } else if ($type == 'y') {
            if (!is_file($storageDir . 'y')) {
                return '无统计结果';
            } else {
                return json_decode(file_get_contents($storageDir . 'y'), true);
            }
        } else if ($type == 'm') {
            if (!is_file($storageDir . 'm')) {
                return '无统计结果';
            } else {
                return json_decode(file_get_contents($storageDir . 'm'), true);
            }
        } else if ($type == 'd') {
            if (!is_file($storageDir . 'd')) {
                return '无统计结果';
            } else {
                return json_decode(file_get_contents($storageDir . 'd'), true);
            }
        } else {
            return false;
        }
    }
    private function storageTerms($terms = [])
    {
        $timestamp = time();
        $nyr = date('Y-m-d', $timestamp);
        $ny = date('Y-m', $timestamp);
        $n = date('Y', $timestamp);
        $dir = __DIR__ . '/statistics/' . $this->IndexName . '/storage/' . $n . '/' . $ny . '/' . $nyr . '/';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        file_put_contents($dir . 'terms', implode(PHP_EOL, $terms) . PHP_EOL, FILE_APPEND);
    }

    public function testAnalyzer($query)
    {
        $text = $query['text'];
        $analyzer = $query['analyzer'];
        return $this->analyzerEntry($text, $analyzer, false, true);
    }

    private function rewrite_query($text, $analyzer = '', $synonym = false)
    {
        $str_len = mb_strlen($text, 'utf-8') + 1;
        $all_zm_len = 0;
        if (preg_match_all('/[a-zA-Z]+/i', $text, $mat)) {
            $all_zm_len = mb_strlen(implode('', $mat[0]), 'utf-8');
        }
        if (($all_zm_len / $str_len) > 0.8) {
            $text_cut = mb_substr($text, 0, $this->subLen + 50);
            if (stristr($text_cut, ' ') && (substr_count($text_cut, ' ') > 5)) {
                $text_cut = substr($text_cut, 0, strripos($text_cut, ' '));
            }
        } else {
            $text_cut = mb_substr($text, 0, $this->subLen);
        }
        $fc_arr = $this->analyzerEntry($text_cut, $analyzer, false, true);
        $this->storageTerms($fc_arr);
        $fc_arr_original =  $fc_arr;
        if (count($fc_arr) > 4) {
            $heightFreq  = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/height_freq_word/height_freq_word_search_diff.txt'));
            $heightFreq  = array_filter($heightFreq);
            $heightFreq = array_slice($heightFreq, 0, 300);
            $stopWordEn  = explode(PHP_EOL, file_get_contents(dirname(__FILE__) . '/windIndexCore/stopword/stopword_en.txt'));
            $stopWordEn  = array_filter($stopWordEn);
            $diffArr = array_merge($heightFreq, $stopWordEn);
            $fc_arr_diff = array_diff($fc_arr, $diffArr);
            if (!empty($fc_arr_diff)) {
                $fc_arr = $fc_arr_diff;
            }
        }
        $fc_arr = $this->filterSymbol_StopWord($text, $fc_arr);

        $fc_arr_j = [
            'synonym_mapping' => [],             'synonym_merge' => [],
        ];
        if ($synonym) {
            $fc_arr_j = $this->getSynonym($fc_arr, $text);
        }
        $fc_arr_j['fc_arr_original'] = $fc_arr_original;
        $fc_arr_j['fc_arr'] = $fc_arr;
        return $fc_arr_j;
    }

    private function getDataById($id = '')
    {
        $id = ceil((int)$id);
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';

        $pdo = new PDO_sqlite($dir);
        $sql = "SELECT * FROM $this->IndexName where $this->primarykey='$id';";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            return json_decode($this->systemDecompression($resRow['doc']), true);
        } else {
            return [];
        }
    }

    private function getDataByIds($idStr, $_source = [])
    {
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        $pdo = new PDO_sqlite($dir);
        $primarykey = $this->mapping['properties']['primarykey'];
        $idStr = str_replace("'", '', $idStr);
        $ids = explode(',', $idStr);
        $num = count($ids);
        $limit = $this->getOriginalSourceSize;
        $count =  ceil($num / $limit);
        $resListNew = [];
        for ($i = 0; $i < $count; ++$i) {
            $ids_slice = array_slice($ids, $i * $limit, $limit);
            $ids_slice = "'" . implode("','", (array)$ids_slice) . "'";
            $sql = "SELECT * FROM $this->IndexName where $primarykey in ($ids_slice);";
            $resList = $pdo->getAll($sql);
            if ($resList) {
                foreach ($resList as $k => $v) {
                    if (empty(($_source))) {
                        $resListNew[] = json_decode($this->systemDecompression($v['doc']), true);
                    } else {
                        $row = json_decode($this->systemDecompression($v['doc']), true);
                        $tempRow = [];
                        foreach ($row as $k2 => $r) {
                            if (in_array($k2, $_source) || ($k2 == $primarykey)) {
                                $tempRow[$k2] = $r;
                            }
                        }
                        if (!empty($tempRow)) {
                            $resListNew[] = $tempRow;
                        }
                    }
                }
            }
        }
        return $resListNew;
    }
    private $isSortByScoreOfBm25 = false;

    public function onResSortByScore()
    {
        $this->isSortByScoreOfBm25 = true;
    }

    public function closeResSortByScore()
    {
        $this->isSortByScoreOfBm25 = false;
    }

    private function bool_analyzer($queryList)
    {
        if (isset($queryList['bool'])) {
            foreach ($queryList['bool'] as $type => $p) {
                $typeArr = ['must', 'should', 'must_not', 'filter'];

                if (in_array($type, $typeArr)) {
                    if (isset($queryList['bool'][$type]['match']) && is_array($queryList['bool'][$type]['match'])) {
                        foreach ($queryList['bool'][$type]['match'] as $k => $arr) {
                            $queryList['bool'][$type]['match'][$k] = [];
                            foreach ($arr as $fd => $q) {
                                $text = $q;
                                $fieldType = $this->getFieldType($fd);
                                if ($fieldType == 'text') {
                                    $analyzer = $this->getFieldAnalyzerType($fd);
                                    $fc_arr_j = $this->rewrite_query($text, $analyzer);
                                    $synonymMapping = $fc_arr_j['synonym_mapping'];
                                    $synonym = $fc_arr_j['synonym_merge'];
                                    $fc_arr_original = $fc_arr_j['fc_arr_original'];
                                    $fc_arr = $fc_arr_j['fc_arr'];
                                } else {
                                    $synonymMapping = [];
                                    $synonym = [];
                                    $fc_arr = [$text];
                                    $fc_arr_original = [$text];
                                }
                                $queryList['bool'][$type]['match'][$k][] = [
                                    'fd' => $fd,
                                    'fc_arr' => $fc_arr,
                                    'synonym_mapping' => $synonymMapping,
                                    'synonym' => $synonym,
                                    'fc_arr_original' => $fc_arr_original,
                                ];
                            }
                        }
                        $temp = [];
                        foreach ($queryList['bool'][$type]['match'] as $k => $v) {
                            if (is_array($v) && !empty($v)) {
                                foreach ($v as $f) {
                                    if (is_array($f)) {
                                        $temp[] = $f;
                                    }
                                }
                            }
                        }
                        $queryList['bool'][$type]['match'] = $temp;
                    } else if (count($queryList['bool'][$type]) > 0) {
                        foreach ($queryList['bool'][$type] as $k => $bool_list) {
                            $queryList['bool'][$type][$k] = $this->bool_analyzer($bool_list);
                        }
                    }
                }
            }
        }
        return $queryList;
    }

    private function normalSearch($query)
    {
        if (!is_array($query)) {
            $this->throwWindException('搜索参数是无效的，必须是一个数组', 0);
        }
        $queryList = [];
        if (isset($query['match'])) {
            $queryList['mode'] = 'match';
            $queryList['field'] = $query['match']['field']['name'];
            $queryList['text'] = $query['match']['field']['query'];
            $queryList['filter'] = isset($query['match']['filter']) ? $query['match']['filter'] : [];
            $queryList['highlight'] = isset($query['match']['field']['highlight']) ? $query['match']['field']['highlight'] : false;
            $queryList['desensitization'] = isset($query['match']['desensitization']) ? $query['match']['desensitization'] : false;
            $queryList['_source'] = isset($query['match']['_source']) ? $query['match']['_source'] : [];
            $queryList['analyzer'] = isset($query['match']['field']['analyzer']) ? $query['match']['field']['analyzer'] : '';
            $queryList['synonym'] = isset($query['match']['field']['synonym']) ? $query['match']['field']['synonym'] : false;
            $queryList['minimum_should_match'] = isset($query['match']['field']['minimum_should_match']) ? $query['match']['field']['minimum_should_match'] : false;

            $queryList['operator'] = isset($query['match']['field']['operator']) ? $query['match']['field']['operator'] : 'or';
            if ($queryList['operator'] != 'and' && $queryList['operator'] != 'or') {
                $queryList['operator'] = 'or';
            }
            $queryList['sort'] = isset($query['match']['sort']) ? $query['match']['sort'] : [];

            $queryList['page'] = ((int)$query['match']['page'] > 0) ? ceil((int)$query['match']['page']) : 1;
            $queryList['list_rows'] = $query['match']['list_rows'];
        } else if (isset($query['match_all'])) {
            $queryList['mode'] = 'match_all';
            $queryList['order'] = isset($query['match_all']['order']) ? $query['match_all']['order'] : 'asc';
            if ($queryList['order'] !== 'desc' && $queryList['order'] !== 'asc') {
                $queryList['order'] = 'asc';
            }
            $queryList['desensitization'] = isset($query['match_all']['desensitization']) ? $query['match_all']['desensitization'] : false;
            $queryList['page'] = ((int)$query['match_all']['page'] > 0) ? ceil((int)$query['match_all']['page']) : 1;
            $queryList['list_rows'] = $query['match_all']['list_rows'];
            $queryList['_source'] = isset($query['match_all']['_source']) ? $query['match_all']['_source'] : [];
        } else if (isset($query['match_range'])) {
            $queryList['mode'] = 'match_range';
            $queryList['range'] = $query['match_range']['range'];
            $queryList['highlight'] = [];
            $queryList['_source'] = isset($query['match_range']['_source']) ? $query['match_range']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_range']['desensitization']) ? $query['match_range']['desensitization'] : false;
            $queryList['sort'] = isset($query['match_range']['sort']) ? $query['match_range']['sort'] : [];
            $queryList['page'] = ((int)$query['match_range']['page'] > 0) ? ceil((int)$query['match_range']['page']) : 1;
            $queryList['list_rows'] = $query['match_range']['list_rows'];
        } else if (isset($query['match_terms'])) {
            $queryList['mode'] = 'match_terms';
            $queryList['field'] = $query['match_terms']['field']['name'];
            $queryList['text'] = (array)$query['match_terms']['field']['terms'];
            $queryList['highlight'] = isset($query['match_terms']['field']['highlight']) ? $query['match_terms']['field']['highlight'] : false;
            $queryList['_source'] = isset($query['match_terms']['_source']) ? $query['match_terms']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_terms']['desensitization']) ? $query['match_terms']['desensitization'] : false;
            $queryList['operator'] = isset($query['match_terms']['operator']) ? $query['match_terms']['operator'] : 'or';
            if ($queryList['operator'] != 'and' && $queryList['operator'] != 'or') {
                $queryList['operator'] = 'or';
            }
            $queryList['sort'] = isset($query['match_terms']['sort']) ? $query['match_terms']['sort'] : [];
            $queryList['page'] = ((int)$query['match_terms']['page'] > 0) ? ceil((int)$query['match_terms']['page']) : 1;
            $queryList['list_rows'] = $query['match_terms']['list_rows'];
        } else if (isset($query['match_prefix'])) {
            $queryList['mode'] = 'match_prefix';
            $queryList['match'] = isset($query['match_prefix']['field']['match']) ? $query['match_prefix']['field']['match'] : false;
            $queryList['field'] = $query['match_prefix']['field']['name'];
            $queryList['text'] = $query['match_prefix']['field']['query'];
            $queryList['highlight'] = $query['match_prefix']['field']['highlight'];
            $queryList['_source'] = isset($query['match_prefix']['_source']) ? $query['match_prefix']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_prefix']['desensitization']) ? $query['match_prefix']['desensitization'] : false;
            $queryList['sort'] = isset($query['match_prefix']['sort']) ? $query['match_prefix']['sort'] : [];
            $queryList['page'] = ((int)$query['match_prefix']['page'] > 0) ? ceil((int)$query['match_prefix']['page']) : 1;
            $queryList['list_rows'] = $query['match_prefix']['list_rows'];
        } else if (isset($query['match_suffix'])) {
            $queryList['mode'] = 'match_suffix';
            $queryList['match'] = isset($query['match_suffix']['field']['match']) ? $query['match_suffix']['field']['match'] : false;
            $queryList['field'] = $query['match_suffix']['field']['name'];
            $queryList['text'] = $query['match_suffix']['field']['query'];
            $queryList['highlight'] = $query['match_suffix']['field']['highlight'];
            $queryList['_source'] = isset($query['match_suffix']['_source']) ? $query['match_suffix']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_suffix']['desensitization']) ? $query['match_suffix']['desensitization'] : false;
            $queryList['sort'] = isset($query['match_suffix']['sort']) ? $query['match_suffix']['sort'] : [];
            $queryList['page'] = ((int)$query['match_suffix']['page'] > 0) ? ceil((int)$query['match_suffix']['page']) : 1;
            $queryList['list_rows'] = $query['match_suffix']['list_rows'];
        } else if (isset($query['match_prefix_suffix'])) {
            $queryList['mode'] = 'match_prefix_suffix';
            $queryList['match'] = isset($query['match_prefix_suffix']['field']['match']) ? $query['match_prefix_suffix']['field']['match'] : false;
            $queryList['field'] = $query['match_prefix_suffix']['field']['name'];
            $queryList['text'] = $query['match_prefix_suffix']['field']['query'];
            $queryList['highlight'] = $query['match_prefix_suffix']['field']['highlight'];
            $queryList['_source'] = isset($query['match_prefix_suffix']['_source']) ? $query['match_prefix_suffix']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_prefix_suffix']['desensitization']) ? $query['match_prefix_suffix']['desensitization'] : false;
            $queryList['sort'] = isset($query['match_prefix_suffix']['sort']) ? $query['match_prefix_suffix']['sort'] : [];
            $queryList['page'] = ((int)$query['match_prefix_suffix']['page'] > 0) ? ceil((int)$query['match_prefix_suffix']['page']) : 1;
            $queryList['list_rows'] = $query['match_prefix_suffix']['list_rows'];
        } else if (isset($query['multi_match'])) {
            $queryList['mode'] = 'multi_match';
            $queryList['field'] = $query['multi_match']['field'];
            $queryList['filter'] = isset($query['multi_match']['filter']) ? $query['multi_match']['filter'] : [];
            $queryList['_source'] = isset($query['multi_match']['_source']) ? $query['multi_match']['_source'] : [];
            $queryList['desensitization'] = isset($query['multi_match']['desensitization']) ? $query['multi_match']['desensitization'] : false;
            $queryList['field_operator'] = isset($query['multi_match']['field_operator']) ? $query['multi_match']['field_operator'] : 'or';
            $queryList['sort'] = isset($query['multi_match']['sort']) ? $query['multi_match']['sort'] : [];
            $queryList['page'] = ((int)$query['multi_match']['page'] > 0) ? ceil((int)$query['multi_match']['page']) : 1;
            $queryList['list_rows'] = $query['multi_match']['list_rows'];
        } else if (isset($query['match_phrase'])) {
            $queryList['mode'] = 'match_phrase';
            $queryList['field'] = $query['match_phrase']['field']['name'];
            $queryList['text'] = $query['match_phrase']['field']['query'];
            $queryList['highlight'] = $query['match_phrase']['field']['highlight'];
            $queryList['_source'] = isset($query['match_phrase']['_source']) ? $query['match_phrase']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_phrase']['desensitization']) ? $query['match_phrase']['desensitization'] : false;
            $queryList['analyzer'] = isset($query['match_phrase']['field']['analyzer']) ? $query['match_phrase']['field']['analyzer'] : '';
            $queryList['operator'] = 'and';
            $queryList['sort'] = isset($query['match_phrase']['sort']) ? $query['match_phrase']['sort'] : [];
            $queryList['page'] = ((int)$query['match_phrase']['page'] > 0) ? ceil((int)$query['match_phrase']['page']) : 1;
            $queryList['list_rows'] = $query['match_phrase']['list_rows'];
            $queryList['phrase'] = true;
        } else if (isset($query['match_bool'])) {
            $queryList['mode'] = 'match_bool';
            $queryList['bool'] = $query['match_bool']['bool'];
            $queryList['highlight'] = $query['match_bool']['highlight'];
            $queryList['_source'] = isset($query['match_bool']['_source']) ? $query['match_bool']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_bool']['desensitization']) ? $query['match_bool']['desensitization'] : false;
            $queryList['sort'] = isset($query['match_bool']['sort']) ? $query['match_bool']['sort'] : [];
            $queryList['page'] = ((int)$query['match_bool']['page'] > 0) ? ceil((int)$query['match_bool']['page']) : 1;
            $queryList['list_rows'] = $query['match_bool']['list_rows'];
        } else if (isset($query['match_primarykey'])) {
            $queryList['mode'] = 'match_primarykey';
            $queryList['primarykey'] = isset($query['match_primarykey']['primarykey']) ? $query['match_primarykey']['primarykey'] : [];
            $queryList['_source'] = isset($query['match_primarykey']['_source']) ? $query['match_primarykey']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_primarykey']['desensitization']) ? $query['match_primarykey']['desensitization'] : false;
        } else if (isset($query['match_rand'])) {
            $queryList['mode'] = 'match_rand';
            $queryList['size'] = ((int)$query['match_rand']['size'] > 0) ? ceil((int)$query['match_rand']['size']) : 0;
            $queryList['_source'] = isset($query['match_rand']['_source']) ? $query['match_rand']['_source'] : [];
            $queryList['desensitization'] = isset($query['match_rand']['desensitization']) ? $query['match_rand']['desensitization'] : false;
        } else if (isset($query['match_fuzzy'])) {
            $queryList['mode'] = 'match_fuzzy';
            $queryList['field'] = $query['match_fuzzy']['field']['name'];
            $queryList['text'] = $query['match_fuzzy']['field']['query'];
            $queryList['filter'] = isset($query['match_fuzzy']['filter']) ? $query['match_fuzzy']['filter'] : [];
            $queryList['highlight'] = isset($query['match_fuzzy']['field']['highlight']) ? $query['match_fuzzy']['field']['highlight'] : false;
            $queryList['desensitization'] = isset($query['match_fuzzy']['desensitization']) ? $query['match_fuzzy']['desensitization'] : false;
            $queryList['_source'] = isset($query['match_fuzzy']['_source']) ? $query['match_fuzzy']['_source'] : [];
            $queryList['analyzer'] = isset($query['match_fuzzy']['field']['analyzer']) ? $query['match_fuzzy']['field']['analyzer'] : '';
            $queryList['synonym'] = isset($query['match_fuzzy']['field']['synonym']) ? $query['match_fuzzy']['field']['synonym'] : false;
            $queryList['minimum_should_match'] = isset($query['match_fuzzy']['field']['minimum_should_match']) ? $query['match_fuzzy']['field']['minimum_should_match'] : false;

            $queryList['operator'] = isset($query['match_fuzzy']['field']['operator']) ? $query['match_fuzzy']['field']['operator'] : 'or';
            if ($queryList['operator'] != 'and' && $queryList['operator'] != 'or') {
                $queryList['operator'] = 'or';
            }
            $queryList['sort'] = isset($query['match_fuzzy']['sort']) ? $query['match_fuzzy']['sort'] : [];
            $queryList['page'] = ((int)$query['match_fuzzy']['page'] > 0) ? ceil((int)$query['match_fuzzy']['page']) : 1;
            $queryList['list_rows'] = $query['match_fuzzy']['list_rows'];
        } else {
            $getSearchMode = array_keys($query)[0];
            $this->throwWindException($getSearchMode . ' 是不支持的搜索模式', 0);
        }

        if ($queryList['mode'] == 'multi_match') {
            foreach ($queryList['field'] as $k => $p) {
                $text = isset($p['query']) ? $p['query'] : '';
                $fd = isset($p['name']) ? $p['name'] : '';
                $fieldType = $this->getFieldType($fd);
                $minimum_should_match = isset($p['minimum_should_match']) ? $p['minimum_should_match'] : false;
                if ($fieldType == 'text') {
                    $synonym = isset($p['synonym']) ? $p['synonym'] : false;
                    $analyzer = isset($p['analyzer']) ? $p['analyzer'] : '';
                    if ($analyzer === '') {
                        $analyzer = $this->getFieldAnalyzerType($fd);
                    }
                    $fc_arr_j = $this->rewrite_query($text, $analyzer, $synonym);
                    $synonymMapping = $fc_arr_j['synonym_mapping'];
                    $synonym = $fc_arr_j['synonym_merge'];
                    $fc_arr_original = $fc_arr_j['fc_arr_original'];
                    $fc_arr = $fc_arr_j['fc_arr'];
                } else {
                    $synonymMapping = [];
                    $synonym = [];
                    $fc_arr_original = [$text];
                    $fc_arr = [$text];
                }
                $queryList['field'][$k]['operator'] = '';
                $queryList['field'][$k]['fc_arr'] = $fc_arr;
                $queryList['field'][$k]['minimum_should_match'] = $minimum_should_match;
                $queryList['field'][$k]['synonym_mapping'] = $synonymMapping;
                $queryList['field'][$k]['synonym'] = $synonym;
                $queryList['field'][$k]['fc_arr_original'] = $fc_arr_original;
            }
        } else if ($queryList['mode'] == 'match_fuzzy') {
            $fieldType = $this->getFieldType($queryList['field']);
            $text = $queryList['text'];
            $synonym = false;
            if ($fieldType == 'text') {
                $analyzer = isset($queryList['analyzer']) ? $queryList['analyzer'] : '';
                if ($analyzer === '') {
                    $analyzer = $this->getFieldAnalyzerType($queryList['field']);
                }
                $fc_arr_j = $this->rewrite_query($text, $analyzer, $synonym);
                $synonymMapping = $fc_arr_j['synonym_mapping'];
                $synonym = $fc_arr_j['synonym_merge'];
                $fc_arr_original = $fc_arr_j['fc_arr_original'];
                $fc_arr = $fc_arr_j['fc_arr'];
            } else {
                $synonymMapping = [];
                $synonym = [];
                $fc_arr_original = [$text];
                $fc_arr = [$text];
            }
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = $synonymMapping;
            $queryList['synonym'] = $synonym;
            $queryList['fc_arr_original'] = $fc_arr_original;
        } else if ($queryList['mode'] == 'match_bool') {
            $queryList = $this->bool_analyzer($queryList);
        } else if ($queryList['mode'] == 'match') {
            $fieldType = $this->getFieldType($queryList['field']);
            $text = $queryList['text'];
            $synonym = $queryList['synonym'];
            if ($fieldType == 'text') {
                $analyzer = isset($queryList['analyzer']) ? $queryList['analyzer'] : '';
                if ($analyzer === '') {
                    $analyzer = $this->getFieldAnalyzerType($queryList['field']);
                }
                $fc_arr_j = $this->rewrite_query($text, $analyzer, $synonym);
                $synonymMapping = $fc_arr_j['synonym_mapping'];
                $synonym = $fc_arr_j['synonym_merge'];
                $fc_arr_original = $fc_arr_j['fc_arr_original'];
                $fc_arr = $fc_arr_j['fc_arr'];
            } else {
                $synonymMapping = [];
                $synonym = [];
                $fc_arr_original = [$text];
                $fc_arr = [$text];
            }
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = $synonymMapping;
            $queryList['synonym'] = $synonym;
            $queryList['fc_arr_original'] = $fc_arr_original;
        } else if ($queryList['mode'] == 'match_range') {
        } else if ($queryList['mode'] == 'match_all') {
        } else if ($queryList['mode'] == 'match_primarykey') {
        } else if ($queryList['mode'] == 'match_rand') {
        } else if ($queryList['mode'] == 'match_terms') {
            $analyzer = 'not';
            $text = (array)$queryList['text'];
            $queryList['fc_arr'] = $text;
            $queryList['synonym_mapping'] = [];
            $queryList['synonym'] = [];
            $queryList['fc_arr_original'] = $text;
        } else if ($queryList['mode'] == 'match_prefix') {
            $text = $queryList['text'];
            if (substr($text, -1) == '*') {
                $text = substr($text, 0, -1);
                $queryList['text'] = $text;
            }
            $analyzer = 'not';
            $fc_arr_j = $this->rewrite_query($text, $analyzer);
            $synonymMapping = $fc_arr_j['synonym_mapping'];
            $synonym = $fc_arr_j['synonym_merge'];
            $fc_arr_original = $fc_arr_j['fc_arr_original'];
            $fc_arr = $fc_arr_j['fc_arr'];
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = $synonymMapping;
            $queryList['synonym'] = $synonym;
            $queryList['fc_arr_original'] = $fc_arr_original;
        } else if ($queryList['mode'] == 'match_suffix') {
            $text = $queryList['text'];
            if (substr($text, 0, 1) == '*') {
                $text = substr($text, 1);
                $queryList['text'] = $text;
            }
            $analyzer = 'not';
            $fc_arr_j = $this->rewrite_query($text, $analyzer);
            $synonymMapping = $fc_arr_j['synonym_mapping'];
            $synonym = $fc_arr_j['synonym_merge'];
            $fc_arr_original = $fc_arr_j['fc_arr_original'];
            $fc_arr = $fc_arr_j['fc_arr'];
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = $synonymMapping;
            $queryList['synonym'] = $synonym;
            $queryList['fc_arr_original'] = $fc_arr_original;
        } else if ($queryList['mode'] == 'match_prefix_suffix') {
            $analyzer = 'not';
            $text = $queryList['text'];
            if (is_string($text)) {
                $terms_list = [$text];
            } else if (is_array($text)) {
                $terms_list = $text;
            } else {
                $terms_list = [];
            }
            $list = [];
            foreach ($terms_list as $t) {
                if (mb_substr($t, -1) == '*') {
                    $list[] = mb_substr($t, 0, -1);
                } else if (mb_substr($t, 0, 1) == '*') {
                    $list[] = mb_substr($t, 1);
                } else {
                    $list[] = $t;
                }
            }
            $fc_arr = (array)array_unique($list);
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = [];
            $queryList['synonym'] = [];
            $queryList['fc_arr_original'] = $fc_arr;
        } else if ($queryList['mode'] == 'match_phrase') {
            $fieldType = $this->getFieldType($queryList['field']);
            $text = $queryList['text'];
            if ($fieldType == 'text') {
                $analyzer = isset($queryList['analyzer']) ? $queryList['analyzer'] : '';
                if ($analyzer === '') {
                    $analyzer = $this->getFieldAnalyzerType($queryList['field']);
                }
                $fc_arr_j = $this->rewrite_query($text, $analyzer);
                $synonymMapping = $fc_arr_j['synonym_mapping'];
                $synonym = $fc_arr_j['synonym_merge'];
                $fc_arr_original = $fc_arr_j['fc_arr_original'];
                $fc_arr = $fc_arr_j['fc_arr'];
            } else {
                $synonymMapping = [];
                $synonym = [];
                $fc_arr_original = [$text];
                $fc_arr = [$text];
            }
            $queryList['fc_arr'] = $fc_arr;
            $queryList['synonym_mapping'] = $synonymMapping;
            $queryList['synonym'] = $synonym;
            $queryList['fc_arr_original'] = $fc_arr_original;
        } else {
            $getSearchMode = array_keys($query)[0];
            $this->throwWindException($getSearchMode . ' 是不支持的搜索模式', 0);
        }
        $this->queryList = $queryList;
        $resArr =  $this->searchEntry($queryList);

        if ($queryList['mode'] == 'multi_match') {
            $highlightField = $queryList['field'];
        } else if ($queryList['mode'] == 'match_bool') {
            // $highlightField = $this->queryList['field'];
            $highlightField = [];
            $this->queryList['field'] = [];
        } else if ($queryList['mode'] == 'match_range') {
            $highlightField = [];
        } else if ($queryList['mode'] == 'match_all') {
            $highlightField = [];
        } else if ($queryList['mode'] == 'match_primarykey') {
            $highlightField = [];
        } else {
            $highlightField = [
                [
                    'name' => $queryList['field'],
                    'query' => $queryList['text'],
                    'operator' => $queryList['operator'],
                    'highlight' => isset($queryList['highlight']) ? $queryList['highlight'] : [],
                    'fc_arr' => $queryList['fc_arr'],
                    'synonym' => $queryList['synonym'],
                    'synonym_mapping' => $queryList['synonym_mapping'],
                    'fc_arr_original' => $queryList['fc_arr_original'],
                ],
            ];
        }
        if (($queryList['mode'] === 'match') || ($queryList['mode'] === 'match_range') || ($queryList['mode'] === 'match_terms') || ($queryList['mode'] === 'multi_match') || ($queryList['mode'] === 'match_bool') || ($queryList['mode'] === 'match_prefix') || ($queryList['mode'] === 'match_suffix') || ($queryList['mode'] === 'match_prefix_suffix') || ($queryList['mode'] === 'match_fuzzy')) {
            if (isset($queryList['sort']) && !empty($queryList['sort'])) {
                $sortField = array_keys($queryList['sort'])[0];
                if (($sortField == '_bm25_score')) {
                    if ($queryList['mode'] == 'match') {
                        $idStrAll = $resArr['intersection']['all_id_str'];
                        $idsAllScore = $resArr['intersection']['all_id_score'];
                        $resList = $this->getDataByIds($idStrAll);
                        $currFieldName = $this->queryList['field'];
                        $primarykey = $this->mapping['properties']['primarykey'];
                        $arr_sort = [];
                        $resList = $this->bm25($resList);

                        $resList = $this->resSort($resList, $queryList);
                    } else {
                        $queryList['sort'] = [];
                        $idStr = $resArr['intersection']['id_str'];
                        $resList = $this->getDataByIds($idStr);
                    }
                } else if (($sortField == '_score')) {
                    $idStr = $resArr['intersection']['id_str'];
                    $id_score = $resArr['intersection']['id_score'];
                    $resList = $this->getDataByIds($idStr);
                    $primarykey = $this->mapping['properties']['primarykey'];
                    $arr_sort = [];
                    foreach ($resList as $s => $row) {
                        $row['_score'] = $id_score[$row[$primarykey]];
                        $arr_sort[$row[$primarykey]] = $row;
                    }
                    $resList = $arr_sort;
                    $resList =  $this->calculate_the_degree_of_term_aggregation($resList, $queryList['field'], $queryList['fc_arr_original']);
                    $resList = $this->usortRes($resList, $field = '_score');
                } else {

                    $idStrAll = $resArr['intersection']['all_id_str'];
                    $idsAllScore = $resArr['intersection']['all_id_score'];
                    $resList = $this->getDataByIds($idStrAll);
                    $resList = $this->resSort($resList, $queryList);
                }
            } else {

                $idStr = $resArr['intersection']['id_str'];
                $resList = $this->getDataByIds($idStr);
            }
        } else if ($queryList['mode'] === 'match_phrase') {
            $idStrAll = $resArr['intersection']['all_id_str'];
            $field = $queryList['field'];
            $query = $queryList['text'];
            $resList = $this->getDataByIds($idStrAll);
            foreach ($resList as $k => $v) {

                if (!stristr($v[$field], $query)) {
                    unset($resList[$k]);
                }
            }
            if (!empty($resList)) {
                $resList = array_values($resList);
            }
            $total = count($resList);
            $resArr['info']['total'] = $total;
            $resList = $this->resSort($resList, $queryList);
        } else if ($queryList['mode'] === 'match_all') {
            $resList = $this->getDataByIds($resArr['ids']);
        } else if ($queryList['mode'] === 'match_primarykey') {
            $resList = $this->getDataByIds($resArr['ids']);
        } else if ($queryList['mode'] === 'match_rand') {
            $resList = $this->getDataByIds($resArr['ids']);
        } else {
            $idStr = $resArr['intersection']['id_str'];
            $resList = $this->getDataByIds($idStr);
        }
        $resList = $this->filterSource($resList, $queryList['_source']);
        $desens = isset($queryList['desensitization']) ? $queryList['desensitization'] : [];
        $resList = $this->desensitization($resList, $desens);
        if ($queryList['mode'] == 'match_all') {
            return [
                'result' => [
                    '_source' => $resList,
                    '_highlight' => [],
                ],
                'info' => $resArr['info'],
            ];
        } else if (($queryList['mode'] === 'match_primarykey') || ($queryList['mode'] === 'match_rand')) {
            if ($resArr['ids'] == '') {
                return [
                    'result' => [
                        '_source' => [],
                        '_highlight' => [],
                    ],
                    'info' => $resArr['info'],
                ];
            } else {
                $primarykey = $this->mapping['properties']['primarykey'];
                $arr_sort = [];
                $id_score = $resArr['id_score'];
                foreach ($resList as $s => $d) {
                    $arr_sort[$d[$primarykey]] = $d;
                }
                if (count($arr_sort) == count($id_score)) {
                    $resListSort = array_replace($id_score, $arr_sort);
                } else {
                    $resListSort = $arr_sort;
                }
                $resArr['info']['total'] = count($resList);
                return [
                    'result' => [
                        '_source' => $resListSort,
                        '_highlight' => [],
                    ],
                    'info' => $resArr['info'],
                ];
            }
        }
        if ((isset($queryList['sort']) && !empty($queryList['sort'])) || ($queryList['mode'] == 'match_phrase')) {
            $resArrHighlight = $this->highLight($resList, $highlightField);
        } else {

            $resArrHighlight = $this->highLight($resList, $highlightField);
            $primarykey = $this->mapping['properties']['primarykey'];
            $arr_sort = [];
            $id_score = $resArr['intersection']['id_score'];
            foreach ($resArrHighlight['_source'] as $s => $d) {
                $d['_score'] = $id_score[$d[$primarykey]];
                $arr_sort[$d[$primarykey]] = $d;
            }
            if (count($arr_sort) == count($id_score)) {
                $originalRes = array_replace($id_score, $arr_sort);
            } else {
                $originalRes = $arr_sort;
            }

            $resArrHighlight['_source'] = $originalRes;
            $originalRes =  $this->calculate_the_degree_of_term_aggregation($resArrHighlight['_source'], $this->queryList['field'], $queryList['fc_arr_original']);

            $originalRes = $this->usortRes($originalRes, $field = '_score');
            $resArrHighlight['_source'] = $originalRes;
        }
        $res = [
            'result' => $resArrHighlight,
            'info' => $resArr['info'],
        ];

        return $res;
    }

    private function getDistance($lat1, $lng1,  $lat2,  $lng2)
    {

        $radLat1 = deg2rad($lat1);
        $radLat2 = deg2rad($lat2);
        $radLng1 = deg2rad($lng1);
        $radLng2 = deg2rad($lng2);
        $a = $radLat1 - $radLat2;
        $b = $radLng1 - $radLng2;
        $s = 2 * asin(sqrt(pow(sin($a / 2), 2) + cos($radLat1) * cos($radLat2) * pow(sin($b / 2), 2))) * 6378.137;
        return $s;
    }

    private function getDistanceRadian($lat1, $lng1,  $lat2,  $lng2)
    {

        $radLat1 = $lat1;
        $radLng1 = $lng1;
        $radLat2 = deg2rad($lat2);
        $radLng2 = deg2rad($lng2);
        $a = $radLat1 - $radLat2;
        $b = $radLng1 - $radLng2;
        $s = 2 * asin(sqrt(pow(sin($a / 2), 2) + cos($radLat1) * cos($radLat2) * pow(sin($b / 2), 2))) * 6378.137;
        return $s;
    }

    private function filterSource($resList, $_source)
    {
        if (!is_array($_source) || empty($_source)) {
            return $resList;
        }
        $primarykey = $this->mapping['properties']['primarykey'];
        $resListNew = [];
        foreach ($resList as $k => $row) {
            $tempRow = [];
            foreach ($row as $k2 => $r) {
                if (in_array($k2, $_source) || $k2 == $primarykey) {
                    $tempRow[$k2] = $r;
                }
                if ($k2 === '_bm25_score' || $k2 === '_score') {
                    $tempRow[$k2] = $r;
                }
            }
            if (!empty($tempRow)) {
                $resListNew[] = $tempRow;
            }
        }
        $resList = $resListNew;
        return $resList;
    }
    private function geoDistanceSearch($query)
    {
        $cacheKey = md5(json_encode($query));
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $this->queryList = $query;
        $field = $query['match_geo']['field']['name'];
        $field_radian = $field . '_radian';
        $baseid = isset($query['match_geo']['baseid']) ? $query['match_geo']['baseid'] : [];
        if (is_string($query['match_geo']['field']['geo_point'])) {
            list($lat, $lon) = explode(',', $query['match_geo']['field']['geo_point']);
        } else {
            $lat = $query['match_geo']['field']['geo_point']['lat'];
            $lon = $query['match_geo']['field']['geo_point']['lon'];
        }
        $lat = (float)$lat;
        $lon = (float)$lon;
        $this->query_point = [$field, [$lat, $lon]];
        $geo_distance = $query['match_geo']['field']['distance'];
        $sort = isset($query['match_geo']['sort']['geo_distance']) ? $query['match_geo']['sort']['geo_distance'] : 'asc';
        if ($sort != 'asc' && $sort != 'desc') {
            $sort = 'asc';
        }
        $source = isset($query['match_geo']['_source']) ? $query['match_geo']['_source'] : [];
        $page = ceil((int)$query['match_geo']['page']);
        $listRows = ceil((int)$query['match_geo']['list_rows']);
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isGeoField($field)) {
            $this->throwWindException($field . ' 字段不属于geo_point类型，无法进行地理空间搜索', 0);
        }

        if (is_array($geo_distance)) {
            $is_array_geo_distance = true;
            if (substr($geo_distance[0], -2) == 'km') {
                $unit = 'km';
                $geo_distance1 = (float)substr($geo_distance[0], 0, -2);
                $geo_distance2 = (float)substr($geo_distance[1], 0, -2);
            } else if (substr($geo_distance[0], -1) == 'm') {
                $unit = 'm';
                $geo_distance1 = (float)substr($geo_distance[0], 0, -1);
                $geo_distance2 = (float)substr($geo_distance[1], 0, -1);
            } else {
                $this->throwWindException('地理空间搜索，“distance”的值必须包含距离单位（km或m）', 0);
            }
        } else {
            $is_array_geo_distance = false;
            if (substr($geo_distance, -2) == 'km') {
                $unit = 'km';
                $geo_distance = (float)substr($geo_distance, 0, -2);
            } else if (substr($geo_distance, -1) == 'm') {
                $unit = 'm';
                $geo_distance = (float)substr($geo_distance, 0, -1);
            } else {
                $this->throwWindException('地理空间搜索，“distance”的值必须包含距离单位（km或m）', 0);
            }
        }
        $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
        $pdo = new PDO_sqlite($dir);
        $dir = $this->indexDir . $this->IndexName . '/index/' . $field_radian . '.db';
        $pdo_radian = new PDO_sqlite($dir);
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        $pdo_source = new PDO_sqlite($dir);
        if ($unit == 'm') {
            if (is_array($geo_distance)) {
                $comparison_distance = $geo_distance2 / 1000;
            } else {
                $comparison_distance = $geo_distance / 1000;
            }
        } else {
            $comparison_distance = $geo_distance;
            if (is_array($geo_distance)) {
                $comparison_distance = $geo_distance2;
            } else {
                $comparison_distance = $geo_distance;
            }
        }
        if ($comparison_distance < 1.5) {
            $prefixLen = 5;
        } else  if ($comparison_distance <= 20) {
            $prefixLen = 4;
        } else if ($comparison_distance <= 80) {
            $prefixLen = 3;
        } else if ($comparison_distance <= 631) {
            $prefixLen = 2;
        } else {
            $prefixLen = 2;
        }
        $geohash = new Geohash;
        $base32 = $geohash::encode($lat, $lon);
        $prefix = substr($base32, 0, $prefixLen);
        if ($prefixLen > 3) {
            $neighbors = $geohash::neighbors($prefix);
            $neighbors = array_values($neighbors);
            $neighbors[] = $prefix;
        } else {
            $neighbors[] = $prefix;
        }
        $result = [];
        $resIdMap = [];
        $resSource = [];
        foreach ($neighbors as $p) {
            $sql = "select ids from $field where term='$p';";
            $resRow = $pdo->getRow($sql);
            if ($resRow) {
                $ids = $resRow['ids'];
                $ids = $this->systemDecompression($ids);
                $ids = $this->differentialDecompression($ids);
                $idsArr = explode(',', $ids);
                if (is_array($baseid) && !empty($baseid)) {
                    $idsArr = $this->multi_skip_intersection($baseid, $idsArr);
                }
                $filterIdsCount = count($idsArr);
                $limit = $this->getOriginalSourceSize;
                $count = ceil($filterIdsCount / $limit);
                for ($i = 0; $i < $count; ++$i) {
                    $ids = array_slice($idsArr, $i * $limit, $limit);
                    $ids = "'" . implode("','", (array)$ids) . "'";
                    $sql = "select * from $field_radian where term in($ids);";
                    $resAll = $pdo_radian->getAll($sql);

                    if ($resAll) {
                        foreach ($resAll as $row) {


                            $radian = $row['ids'];
                            $id = $row['term'];
                            list($latitude, $longitude) = explode(',', $radian);
                            $distance = $this->getDistanceRadian((float)$latitude, (float)$longitude,  $lat,  $lon);
                            if ($is_array_geo_distance) {
                                if ($unit === 'km') {
                                    if (($distance >= $geo_distance1) && ($distance <= $geo_distance2)) {
                                        $result[$id] = [
                                            'id' => $id,
                                            'dis' => round($distance, 2),
                                            'dis_unit' => round($distance, 2) . 'km',
                                        ];
                                        $resIdMap[$id] = $distance;
                                    }
                                } else if ($unit === 'm') {
                                    $distance = $distance * 1000;
                                    if (($distance >= $geo_distance1) && ($distance <= $geo_distance2)) {
                                        $result[$id] = [
                                            'id' => $id,
                                            'dis' => round($distance, 2),
                                            'dis_unit' => round($distance, 2) . 'm',
                                        ];
                                        $resIdMap[$id] = $distance;
                                    }
                                }
                            } else {
                                if ($unit === 'km') {
                                    if ($distance <= $geo_distance) {
                                        $result[$id] = [
                                            'id' => $id,
                                            'dis' => round($distance, 2),
                                            'dis_unit' => round($distance, 2) . 'km',
                                        ];
                                        $resIdMap[$id] = $distance;
                                    }
                                } else if ($unit === 'm') {
                                    $distance = $distance * 1000;
                                    if ($distance <= $geo_distance) {
                                        $result[$id] = [
                                            'id' => $id,
                                            'dis' => round($distance, 2),
                                            'dis_unit' => round($distance, 2) . 'm',
                                        ];
                                        $resIdMap[$id] = $distance;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            unset($resRow);
        }
        $dir  = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        $geoIndex = $dir . '/real_time_data.index';
        if (is_file($geoIndex)) {
            $realData = (array)json_decode(file_get_contents($geoIndex), true);
            foreach ($realData as $id => $radian) {
                $radianArr = explode(',', $radian);
                $latitude = (float)$radianArr[0];
                $longitude = (float)$radianArr[1];
                $distance = $this->getDistanceRadian($latitude, $longitude,  $lat,  $lon);
                if (is_array($geo_distance)) {
                    if ($unit == 'km') {
                        if (($distance >= $geo_distance1) && ($distance <= $geo_distance2)) {
                            $result[$id] = [
                                'id' => $id,
                                'dis' => round($distance, 2),
                                'dis_unit' => round($distance, 2) . 'km',
                            ];
                            $resIdMap[$id] = $distance;
                        }
                    } else if ($unit == 'm') {
                        $distance = $distance * 1000;
                        if (($distance >= $geo_distance1) && ($distance <= $geo_distance2)) {
                            $result[$id] = [
                                'id' => $id,
                                'dis' => round($distance, 2),
                                'dis_unit' => round($distance, 2) . 'm',
                            ];
                            $resIdMap[$id] = $distance;
                        }
                    }
                } else {
                    if ($unit == 'km') {
                        if ($distance <= $geo_distance) {
                            $result[$id] = [
                                'id' => $id,
                                'dis' => round($distance, 2),
                                'dis_unit' => round($distance, 2) . 'km',
                            ];
                            $resIdMap[$id] = $distance;
                        }
                    } else if ($unit == 'm') {
                        $distance = $distance * 1000;
                        if ($distance <= $geo_distance) {
                            $result[$id] = [
                                'id' => $id,
                                'dis' => round($distance, 2),
                                'dis_unit' => round($distance, 2) . 'm',
                            ];
                            $resIdMap[$id] = $distance;
                        }
                    }
                }
            }
        }
        if (!empty($result)) {
            if ($sort == 'asc') {
                asort($resIdMap);
            } else {
                arsort($resIdMap);
            }
            $total = count($resIdMap);
            $pageRes = array_slice($resIdMap, ($page - 1) * $listRows, $listRows, true);
            $idStr = implode(',', array_keys($pageRes));
            $res = $this->getDataByIds($idStr);

            $primarykey = $this->mapping['properties']['primarykey'];
            $tempRow = [];
            foreach ($res as $k => $v) {
                $tempRow[$k] = $v;
                $tempRow[$k]['_distance'] = $result[$v[$primarykey]]['dis'];
                $tempRow[$k]['_dis_unit'] = $result[$v[$primarykey]]['dis_unit'];
            }
            $resNew = [];
            foreach ($tempRow as $k => $v) {
                $resNew[$v[$primarykey]] = $v;
            }

            if (count($pageRes) == count($resNew)) {
                $res = array_replace($pageRes, $resNew);
            } else {
                $res = $resNew;
            }
            $res = $this->filterSource($res, $source);
            $result_info = [
                'result' => [
                    '_source' => $res,
                    '_highlight' => [],
                ],
                'info' => [
                    'total' => $total,
                ],
            ];
        } else {
            $result_info = [
                'result' => [],
                'info' => [
                    'total' => 0
                ],
            ];
        }
        $this->cache($cacheKey, json_encode($result_info));
        return $result_info;
    }

    private function getRealTimeData($field, $t)
    {
        $idsArrTemp = false;
        $szm = $this->getFirstLetter($t);
        $dp_index_block_dir = $this->indexDir . $this->IndexName . '/index/real_time_index/' . $field . '/block';
        $index_file = $dp_index_block_dir . '/' . $szm . '/dp.index';
        if (is_file($index_file)) {
            $dp_arr = json_decode(file_get_contents($index_file), true);
            if (is_array($dp_arr) && isset($dp_arr[$t])) {
                $isTextField = $this->isTextField($field);
                if ($this->primarykeyType == 'Int_Incremental') {
                    if ($isTextField) {
                        $decimalArTemp = [];
                        foreach ($dp_arr[$t] as $s => $i) {
                            $decimalArr = $this->bitmapInverse($s, $i);
                            foreach ($decimalArr as $e) {
                                $decimalArTemp[] = $e;
                            }
                        }
                        $idsArrTemp = implode(',', $decimalArTemp);
                    } else {
                        $idsArrTemp = $dp_arr[$t];
                    }
                } else {
                    $idsArrTemp = $dp_arr[$t];
                }
            }
        }
        return $idsArrTemp;
    }

    private function getKeywordAndTextIndexData($field, $terms = [], $is_return_all = false)
    {
        if ($this->primarykeyType == 'Int_Incremental') {
            $isKeyWordField = $this->isKeyWordField($field);
            if ($isKeyWordField) {
                $field_select = $field;
            } else {
                $field_select = $field . '_postinglist';
            }
        } else {
            $field_select = $field;
        }
        $indexDataDir = $this->indexDir . $this->IndexName . '/index/' . $field_select . '.db';
        $pdo = new PDO_sqlite($indexDataDir);
        $idsArr = [];
        if (is_string($terms)) {
            $sql = "select ids from $field_select where term='$terms';";
            $resRow = $pdo->getRow($sql);
            if ($resRow) {
                $ids_gzinf = $this->systemDecompression($resRow['ids']);
                $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                $ids = explode(',', $ids_gzinf);
            } else {
                $ids = false;
            }
            if (is_array($ids)) {
                $idsArr = array_merge($idsArr, $ids);
            }
            $realTimeData = $this->getRealTimeData($field, $terms);
            if ($realTimeData !== false) {
                $idsArr = array_merge($idsArr, explode(',', $realTimeData));
            }
        } else if (is_array($terms)) {
            if (!empty($terms)) {
                foreach ($terms as $t) {
                    $sql = "select ids from $field_select where term='$t';";
                    $resRow = $pdo->getRow($sql);
                    if ($resRow) {
                        $ids_gzinf = $this->systemDecompression($resRow['ids']);
                        $ids_gzinf = $this->differentialDecompression($ids_gzinf);
                        $ids = explode(',', $ids_gzinf);
                    } else {
                        $ids = false;
                    }
                    if (is_array($ids)) {
                        $idsArr = array_merge($idsArr, $ids);
                    }
                    $realTimeData = $this->getRealTimeData($field, $t);
                    if ($realTimeData !== false) {
                        $idsArr = array_merge($idsArr, explode(',', $realTimeData));
                    }
                }
            }
        }
        if ($is_return_all) {
            return $idsArr;
        } else {
            return array_unique($idsArr);
        }
    }

    private function aggsGroupStatisticsSimp($queryList)
    {
        if (!$this->isField($queryList['field'])) {
            $this->throwWindException($queryList['field'] . ' 字段不存在', 0);
        }
        if (!$this->isKeyWordField($queryList['field'])) {
            $this->throwWindException($queryList['field'] . ' 字段不属于keyword类型，无法进行分组聚合', 0);
        }
        $dir = $this->getStorageDir();
        $dir = $dir . 'baseData/' . $this->IndexName . '.db';
        $pdo = new PDO_sqlite($dir);
        $countContainer = [];
        if ($queryList['match'] && !empty($queryList['match'])) {
            $filterIds = $this->getKeywordIndexDataEntry($queryList);
            if (!empty($filterIds)) {
                $filterIdsCount = count($filterIds);
                $limit = $this->getOriginalSourceSize;
                $step = ceil($filterIdsCount / $limit);
                for ($i = 0; $i < $step; ++$i) {
                    $ids = array_slice($filterIds, $i * $limit, $limit);
                    $ids = "'" . implode("','", (array)$ids) . "'";
                    $sql = "select * from $this->IndexName where $this->primarykey in($ids);";
                    $resAll = $pdo->getAll($sql);
                    if ($resAll) {
                        foreach ($resAll as $resRow) {
                            $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                            $fieldContent = $resRow[$queryList['field']];
                            if (!isset($countContainer[$fieldContent])) {
                                $countContainer[$fieldContent] = 1;
                            } else {
                                ++$countContainer[$fieldContent];
                            }
                        }
                    }
                }
            }
        } else {
            $min_max_id = $this->get_minid_maxid($pdo);
            if ($min_max_id) {
                $minId = $min_max_id[0];
                $maxid = $min_max_id[1];
                $docNum = $min_max_id[2];
                $step = ceil($docNum / 500);
                for ($i = 1; $i < $step; ++$i) {
                    $b1 = $minId + ($i - 1) * 500;
                    $b2 = $minId + $i * 500 - 1;
                    $sql = "select * from $this->IndexName where $this->sys_primarykey between $b1 and $b2;";
                    $resAll = $pdo->getAll($sql);
                    if ($resAll) {
                        foreach ($resAll as $resRow) {
                            $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                            $fieldContent = $resRow[$queryList['field']];
                            if (!isset($countContainer[$fieldContent])) {
                                $countContainer[$fieldContent] = 1;
                            } else {
                                ++$countContainer[$fieldContent];
                            }
                        }
                    }
                }
            }
        }
        $resArr = [];
        if (!empty($countContainer)) {
            foreach ($countContainer as $k => $v) {
                $resArr[] = [
                    'key' => $k,
                    '_count' => $v,
                ];
            }
        }
        return $resArr;
    }

    private function getKeywordIndexDataEntry($queryList)
    {
        $filterIds = [];
        if ($queryList['match'] && !empty($queryList['match'])) {
            foreach ($queryList['match'] as $fd => $v) {
                $matchField = $fd;
                $matchVal = $v;
                if ($matchField) {
                    if (!$this->isIndexField($matchField)) {
                        $this->throwWindException($matchField . ' 字段未配置索引', 0);
                    }
                    if ($this->isKeyWordField($matchField)) {
                        $filterIds[] = $this->getKeywordAndTextIndexData($matchField, $matchVal);
                    } else if ($this->isTextField($matchField)) {
                        $filterIds[] = $this->getKeywordAndTextIndexData($matchField, $matchVal);
                    } else {
                        $this->throwWindException($matchField . ' 字段不是keyword或text类型', 0);
                    }
                }
            }
            if (count($filterIds) > 1) {
                $filterIds = (array)$this->multi_skip_intersection(...$filterIds);
            } else {
                $filterIds = $filterIds[0];
            }
        }
        return $filterIds;
    }

    private function nestAggsGroupStatistics($queryList)
    {
        $groupField = $queryList['groupField'];
        if (empty($this->countContainer)) {
            $dir = $this->getStorageDir();
            $dir = $dir . 'baseData/' . $this->IndexName . '.db';

            $pdo = new PDO_sqlite($dir);
            if ($queryList['match'] && !empty($queryList['match'])) {
                $keywordIds = $this->getKeywordIndexDataEntry($queryList);
                if (!empty($keywordIds)) {
                    $keywordIdsCount = count($keywordIds);
                    $limit = $this->getOriginalSourceSize;
                    $step = ceil($keywordIdsCount / $limit);
                    for ($i = 0; $i < $step; ++$i) {
                        $ids = array_slice($keywordIds, $i * $limit, $limit);
                        $ids = "'" . implode("','", (array)$ids) . "'";
                        $sql = "select * from $this->IndexName where $this->primarykey in($ids);";
                        $resRowAll = $pdo->getAll($sql);
                        if ($resRowAll) {
                            foreach ($resRowAll as $resRow) {
                                $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                $fieldContent = $resRow[$groupField];
                                $countContainer[$fieldContent][] = $resRow;
                            }
                        }
                    }
                    $this->countContainer = $countContainer;
                }
            } else {
                $countContainer = [];











                $min_max_id = $this->get_minid_maxid($pdo);
                if ($min_max_id) {
                    $minId = $min_max_id[0];
                    $maxid = $min_max_id[1];
                    $docNum = $min_max_id[2];
                    $step = ceil($docNum / 500);
                    for ($i = 1; $i < $step; ++$i) {
                        $b1 = $minId + ($i - 1) * 500;
                        $b2 = $minId + $i * 500 - 1;
                        $sql = "select * from $this->IndexName where $this->sys_primarykey between $b1 and $b2;";
                        $resAll = $pdo->getAll($sql);
                        if ($resAll) {
                            foreach ($resAll as $resRow) {
                                $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                                $fieldContent = $resRow[$groupField];
                                $countContainer[$fieldContent][] = $resRow;
                            }
                        }
                    }
                }
                $this->countContainer = $countContainer;
            }
        } else {
            $countContainer = [];
            foreach ($this->countContainer as $k => $v) {
                foreach ($v as $g => $z) {
                    $fieldContent = $z[$groupField];
                    $countContainer[$k][$fieldContent][] = $z;
                }
            }
            $this->countContainer = $countContainer;
        }
    }

    private function nestAggsGroupAmms($amms, $amms2, $field)
    {
        $countContainer = $this->countContainer;
        $resContair = [];
        if ($this->nestAggsGroupStatisticsCount == 1) {
            if ($amms && $amms2) {
                foreach ($countContainer as $k => $b) {
                    $goup1 = $k;
                    $fieldContent = array_column($b, $field);
                    if ($amms2 == 'avg') {
                        $res = array_sum($fieldContent) / count($fieldContent);
                    } else if ($amms2 == 'max') {
                        $res = max($fieldContent);
                    } else if ($amms2 == 'min') {
                        $res = min($fieldContent);
                    } else if ($amms2 == 'sum') {
                        $res = array_sum($fieldContent);
                    } else {
                        $res = false;
                    }
                    $resContair[$goup1] = $res;
                }
                $resContairNew = [];
                if ($amms == 'avg') {
                    $res = array_sum(array_values($resContair)) / count($resContair);
                    $resContairNew[$field . '_' . $amms2 . '_avg'] = $res;
                } else if ($amms == 'max') {
                    $res = max($resContair);
                    $resContairNew[$field . '_' . $amms2 . '_max'] = [array_search($res, $resContair) => $res];
                } else if ($amms == 'min') {
                    $res = min($resContair);
                    $resContairNew[$field . '_' . $amms2 . '_min'] = [array_search($res, $resContair) => $res];
                } else if ($amms == 'sum') {
                    $res = array_sum(array_values($resContair));
                    $resContairNew[$field . '_' . $amms2 . '_sum'] = $res;
                } else {
                    $res = false;
                }
                $resContair = $resContairNew;
            } else if ($amms && !$amms2) {
                foreach ($countContainer as $k => $b) {
                    $goup1 = $k;
                    $fieldContent = array_column($b, $field);
                    if ($amms == 'avg') {
                        $res = array_sum($fieldContent) / count($fieldContent);
                        $resContair[$goup1] = [
                            $field . '_avg' => [
                                $field => $res
                            ],
                        ];
                    } else if ($amms == 'max') {
                        $res = max($fieldContent);
                        $resContair[$goup1] = [
                            $field . '_max' => [
                                $field => $res
                            ],
                        ];
                    } else if ($amms == 'min') {
                        $res = min($fieldContent);
                        $resContair[$goup1] = [
                            $field . '_min' => [
                                $field => $res
                            ],
                        ];
                    } else if ($amms == 'sum') {
                        $res = array_sum($fieldContent);
                        $resContair[$goup1] = [
                            $field . '_sum' => [
                                $field => $res
                            ],
                        ];
                    } else {
                        $res = false;
                    }
                }
            } else {
                $resContair = $countContainer;
            }
        } else if ($this->nestAggsGroupStatisticsCount == 2) {
            if ($amms && $amms2) {
                foreach ($countContainer as $k => $v) {
                    $goup1 = $k;
                    foreach ($v as $t => $b) {
                        $goup2 = $t;
                        $fieldContent = array_column($b, $field);
                        if ($amms2 == 'avg') {
                            $res = array_sum($fieldContent) / count($fieldContent);
                        } else if ($amms2 == 'max') {
                            $res = max($fieldContent);
                        } else if ($amms2 == 'min') {
                            $res = min($fieldContent);
                        } else if ($amms2 == 'sum') {
                            $res = array_sum($fieldContent);
                        } else {
                            $res = false;
                        }
                        $resContair[$goup1][$goup2] = $res;
                    }
                }
                foreach ($resContair as $k => $v) {
                    if ($amms == 'avg') {
                        $res = array_sum(array_values($v)) / count($v);
                        $resContair[$k] = [
                            $field . '_' . $amms2 . '_avg' => $res,
                        ];
                    } else if ($amms == 'max') {
                        $res = max($v);
                        $resContair[$k] = [
                            $field . '_' . $amms2 . '_max' => [array_search($res, $v) => $res],
                        ];
                    } else if ($amms == 'min') {
                        $res = min($v);
                        $resContair[$k] = [
                            $field . '_' . $amms2 . '_min' => [array_search($res, $v) => $res],
                        ];
                    } else if ($amms == 'sum') {
                        $res = array_sum(array_values($v));
                        $resContair[$k] = [
                            $field . '_' . $amms2 . '_sum' => $res,
                        ];
                    } else {
                        $res = false;
                    }
                }
            } else {
                foreach ($countContainer as $k => $v) {
                    $goup1 = $k;
                    foreach ($v as $t => $b) {
                        $goup2 = $t;
                        $fieldContent = array_column($b, $field);
                        if ($amms == 'avg') {
                            $res = array_sum($fieldContent) / count($fieldContent);
                        } else if ($amms == 'max') {
                            $res = max($fieldContent);
                        } else if ($amms == 'min') {
                            $res = min($fieldContent);
                        } else if ($amms == 'sum') {
                            $res = array_sum($fieldContent);
                        } else {
                            $res = false;
                        }
                        $resContair[$goup1][$goup2] = ['_' . $amms => [$field => $res]];
                    }
                }
            }
        }
        return $resContair;
    }

    private function isField($field)
    {
        $allFieldName = (array)array_column($this->mapping['properties']['all_field'], 'name');
        return in_array($field, $allFieldName);
    }

    private function getFieldType($field)
    {
        $mapping = $this->mapping['properties']['fieldtype_mapping'];
        return isset($mapping[$field]) ? $mapping[$field] : false;
    }

    private function isTextField($field)
    {
        $textField = (array)$this->mapping['properties']['allFieldType']['text'];
        return in_array($field, $textField);
    }

    private function isKeyWordField($field)
    {
        $keyWordField = (array)$this->mapping['properties']['allFieldType']['keyword'];
        return in_array($field, $keyWordField);
    }

    private function isGeoField($field)
    {
        $GeoField = (array)$this->mapping['properties']['availableFieldType']['geo_point'];
        return in_array($field, $GeoField);
    }

    private function isAutoCompletionField($field)
    {
        $AutoCompletionField = (array)$this->mapping['properties']['auto_completion_field'];
        return in_array($field, $AutoCompletionField);
    }

    private function isNumericField($field)
    {
        $keyWordField = (array)$this->mapping['properties']['availableFieldType']['numeric'];
        return in_array($field, $keyWordField);
    }

    private function isIndexField($field)
    {
        $indexField = (array)array_column($this->mapping['properties']['field'], 'name');
        return in_array($field, $indexField);
    }

    private function getFieldAnalyzerType($field)
    {
        $field_analyzer_type = (array)$this->mapping['properties']['field_analyzer_type'];
        return $field_analyzer_type[$field];
    }
    private $countContainer = [];

    private function aggsIndexStatistics($field, $amms)
    {
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }

        if (empty($this->countContainer)) {
            $dir = $this->getStorageDir();
            $dir = $dir . 'baseData/' . $this->IndexName . '.db';
            $pdo = new PDO_sqlite($dir);
            $sql = "SELECT max($this->primarykey) as max_id FROM $this->IndexName;";
            $resRow = $pdo->getRow($sql);
            if ($resRow) {
                $maxId = (int)$resRow['max_id'];
            } else {
                $maxId = 0;
            }
            $sql = "SELECT min($this->primarykey) as min_id FROM $this->IndexName;";
            $resRow = $pdo->getRow($sql);
            if ($resRow) {
                $minId = (int)$resRow['min_id'];
            } else {
                $minId = 0;
            }
            if ($maxId == 0) {
                return [];
            }
            $maxId += 1;
            $docNum = $maxId - $minId;
            if ($docNum < 1) {
                return [];
            }
            $countContainer = [];
            $step = ceil($docNum / 500);
            for ($i = 1; $i < $step; ++$i) {
                $b1 = $minId + ($i - 1) * 500;
                $b2 = $minId + $i * 500;
                $sql = "select * from $this->IndexName where $this->primarykey between $b1 and $b2;";
                $resAll = $pdo->getAll($sql);
                if ($resAll) {
                    foreach ($resAll as $resRow) {
                        $resRow = json_decode($this->systemDecompression($resRow['doc']), true);
                        $fieldContent = (float)$resRow[$field];
                        $countContainer[] = $fieldContent;
                    }
                }
            }
            $this->countContainer = $countContainer;
        }
        $resArr = [];
        if (!empty($this->countContainer)) {
            if ($amms == 'avg') {
                $resArr = [
                    'key' => $field,
                    '_avg' => array_sum($this->countContainer) / count($this->countContainer),
                ];
            } else if ($amms == 'max') {
                $resArr = [
                    'key' => $field,
                    '_max' => max($this->countContainer),
                ];
            } else if ($amms == 'min') {
                $resArr = [
                    'key' => $field,
                    '_min' => min($this->countContainer),
                ];
            } else if ($amms == 'sum') {
                $resArr = [
                    'key' => $field,
                    '_sum' => array_sum($this->countContainer),
                ];
            }
        }
        return $resArr;
    }
    private $idfContainer = [];

    private function bm25($res)
    {
        $terms = $this->queryList['fc_arr_original'];
        $field = $this->queryList['field'];

        $freq = 1;
        $avgdl = 100;
        $k = 1.2;
        $b = 0.75;
        $fieldArr = array_column($res, $field);
        $avgdl = (mb_strlen(implode('', $fieldArr)) / 2) / count($fieldArr);
        foreach ($res as $i => $v) {
            $fieldContent = $v[$field];
            $dl = mb_strlen($fieldContent, 'utf-8') / 2;
            $scoreContainer = [];
            $score = 0;
            foreach ($terms as $t) {
                $freq = substr_count($fieldContent, $t);
                $idf = isset($this->idfContainer[$t]) ? (float)$this->idfContainer[$t] : 5;
                $scoreContainer[] =  $idf * ($freq / ($freq + $k * (1 - $b + $b * $dl / $avgdl)));
            }
            $score = array_sum($scoreContainer);
            $res[$i]['_bm25_score'] = (float)$score;
        }




        return $res;
    }
    private function countDis($resArr)
    {
        if (!empty($this->query_point)) {
            $field = $this->query_point[0];
            foreach ($resArr as $k => $v) {
                $location = $v[$field];
                if (is_array($location)) {
                    $latlonArr = $location;
                } else {
                    $latlonArr = explode(',', $location);
                }
                $latitude = (float)$latlonArr[0];
                $longitude = (float)$latlonArr[1];
                $distance = $this->getDistance($this->query_point[1][0], $this->query_point[1][1],  $latitude,  $longitude);
                $resArr[$k]['_distance'] = $distance;
                $resArr[$k]['_dis_unit'] = 'km';
            }
        } else {
            foreach ($resArr as $k => $v) {
                $resArr[$k]['_distance'] = 0;
                $resArr[$k]['_dis_unit'] = 'km';
            }
        }
        return $resArr;
    }

    private function resSort($resList, $queryList)
    {
        if (isset($queryList['sort']) && !empty($queryList['sort'])) {
            $sortField = array_keys($queryList['sort'])[0];
            if ($sortField === '_distance') {
                $resList = $this->countDis($resList);
            }
            $howSort = $queryList['sort'][$sortField];
            if ($howSort != 'asc' && $howSort != 'desc') {
                return [
                    'error' => 1,
                    'msg' => '排序方式只能是asc（升序）或desc（降序）'
                ];
            }
            $page = $queryList['page'];
            $listRows = $queryList['list_rows'];
            $sortFieldList = array_column($resList, $sortField);
            if (count($sortFieldList) == count($resList)) {
                if ($howSort == 'desc') {
                    $resList = $this->usortRes($resList, $field = $sortField, 'desc');
                } else {
                    $resList = $this->usortRes($resList, $field = $sortField, 'asc');
                }
                $resList = array_slice($resList, ($page - 1) * $listRows, $listRows);
            } else {
                return [
                    'error' => 1,
                    'msg' => '一些源数据无<' . $sortField . '>字段，无法进行排序'
                ];
            }
        } else {
            $page = $queryList['page'];
            $listRows = $queryList['list_rows'];
            $resList = array_slice($resList, ($page - 1) * $listRows, $listRows);
        }
        return $resList;
    }
    private $nestAggsGroupStatisticsCount = 0;
    private $query = false;

    private function selfCallAggsSearch($query)
    {
        $resList = [];
        $aggsGroup = $query['aggs_group'];
        $queryList['mode'] = 'aggs_group';
        $groupField = $query['aggs_group']['field']['name'];
        $match = isset($query['aggs_group']['field']['match']) ? $query['aggs_group']['field']['match'] : false;
        if (!$this->isField($groupField)) {
            $this->throwWindException($groupField . ' 字段不存在', 0);
        }
        if (!$this->isKeyWordField($groupField)) {
            $this->throwWindException($groupField . ' 字段不属于keyword类型，无法进行分组聚合', 0);
        }
        ++$this->nestAggsGroupStatisticsCount;
        $param = [
            'groupField' => $groupField,
            'match' => $match,
        ];
        $this->nestAggsGroupStatistics($param);
        if (isset($aggsGroup['aggs_group']) && $this->nestAggsGroupStatisticsCount < 2) {
            $resList = $this->selfCallAggsSearch($aggsGroup);
        } else {
            if (isset($aggsGroup['avg'])) {
                $amms = 'avg';
            } else if ($aggsGroup['max']) {
                $amms = 'max';
            } else if ($aggsGroup['min']) {
                $amms = 'min';
            } else if ($aggsGroup['sum']) {
                $amms = 'sum';
            } else {
                $amms = false;
                return $this->countContainer;
            }
            if (isset($aggsGroup[$amms]['name'])) {
                $field = $aggsGroup[$amms]['name'];
                $resList = $this->nestAggsGroupAmms($amms, false, $field);
            } else {
                if (isset($aggsGroup[$amms]['avg'])) {
                    $amms2 = 'avg';
                } else if ($aggsGroup[$amms]['max']) {
                    $amms2 = 'max';
                } else if ($aggsGroup[$amms]['min']) {
                    $amms2 = 'min';
                } else if ($aggsGroup[$amms]['sum']) {
                    $amms2 = 'sum';
                } else {
                    $amms2 = false;
                }
                if (!$amms || !$amms2) {
                    return $this->countContainer;
                }
                $field = $aggsGroup[$amms][$amms2]['name'];
                $resList = $this->nestAggsGroupAmms($amms, $amms2, $field);
            }
        }
        return $resList;
    }

    private function aggsSearch($query)
    {
        $cacheKey = md5(json_encode($query));
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        if (!$query) {
            $this->query = $query;
        }
        $resList = [];
        if (isset($query['aggs_group_simp'])) {
            $queryList['field'] = $query['aggs_group_simp']['field']['name'];
            $queryList['match'] = isset($query['aggs_group_simp']['field']['match']) ? $query['aggs_group_simp']['field']['match'] : false;
            $queryList['sort'] = isset($query['aggs_group_simp']['sort']) ? $query['aggs_group_simp']['sort'] : [];
            $queryList['page'] = ((int)$query['aggs_group_simp']['page'] > 0) ? ceil((int)$query['aggs_group_simp']['page']) : 1;
            $queryList['list_rows'] = $query['aggs_group_simp']['list_rows'];
            $resList = $this->aggsGroupStatisticsSimp($queryList);
            $total = count($resList);
            $resList = $this->resSort($resList, $queryList);
            $resList = [
                'result' => $resList,
                'info' => [
                    'page' => $queryList['page'],
                    'list_rows' => $queryList['list_rows'],
                    'total' => $total,
                    'total_page' => ceil($total / $queryList['list_rows']),
                ],
            ];
        } else if (isset($query['aggs_group'])) {
            $resList = $this->selfCallAggsSearch($query);
            $total = count($resList);
            $queryList['page'] = ((int)$query['aggs_group']['page'] > 0) ? ceil((int)$query['aggs_group']['page']) : 1;
            $queryList['list_rows'] = $query['aggs_group']['list_rows'];
            $resList = array_slice($resList, ($queryList['page'] - 1) * $queryList['list_rows'], $queryList['list_rows'], true);
            $resList = [
                'result' => $resList,
                'info' => [
                    'page' => $queryList['page'],
                    'list_rows' => $queryList['list_rows'],
                    'total' => $total,
                    'total_page' => ceil($total / $queryList['list_rows']),
                ],
            ];
        } else if (isset($query['aggs_metrics'])) {
            $resList = [];
            $queryList['mode'] = 'aggs_metrics';
            if (is_array($query['aggs_metrics']['aggs'])) {
                foreach ($query['aggs_metrics']['aggs'] as $k => $v) {
                    $field = $query['aggs_metrics']['aggs'][$k]['field']['name'];
                    $res = $this->aggsIndexStatistics($field, $k);
                    if (!empty($res)) {
                        $resList[] = $res;
                    }
                }
            }
            $total = count($resList);
            $queryList['page'] = ((int)$query['aggs_metrics']['page'] > 0) ? ceil((int)$query['aggs_metrics']['page']) : 1;
            $queryList['list_rows'] = $query['aggs_metrics']['list_rows'];
            $resList = array_slice($resList, ($queryList['page'] - 1) * $queryList['list_rows'], $queryList['list_rows'], true);
            $resList = [
                'result' => $resList,
                'info' => [
                    'page' => $queryList['page'],
                    'list_rows' => $queryList['list_rows'],
                    'total' => $total,
                    'total_page' => ceil($total / $queryList['list_rows']),
                ],
            ];
        }
        $this->cache($cacheKey, json_encode($resList));
        return $resList;
    }

    private function getPrefix($str, $maxlen = 7)
    {
        $replace = [
            ' ' => '',
            PHP_EOL => '',
        ];
        $str = trim(strtr($str, $replace));
        $str = preg_replace("/\s+|\|/", "", $str);
        $autoCompletionStr = strtolower($str);
        $strlen = mb_strlen($autoCompletionStr);
        $maxlen = ($strlen > ($maxlen - 1)) ? $maxlen : $strlen;
        return mb_substr($autoCompletionStr, 0, $maxlen);
    }

    private function autoCompletionSearch($query)
    {
        $cacheKey = md5(json_encode($query));
        $resultCache = $this->getCache($cacheKey);
        if ($resultCache) {
            return json_decode($resultCache, true);
        }
        $this->queryList = $query;
        $field = $query['match_auto_completion']['field']['name'];
        $text = $query['match_auto_completion']['field']['query'];
        $num = (int)$query['match_auto_completion']['num'];
        $num = ($num < 1) ? 1 : $num;
        if (!$this->isField($field)) {
            $this->throwWindException($field . ' 字段不存在', 0);
        }
        if (!$this->isAutoCompletionField($field)) {
            $this->throwWindException($field . ' 字段未配置成auto_completion，无法进行“自动补全”搜索', 0);
        }
        $prefix = $this->getPrefix($text, $this->autoCompletePrefixLen);
        $field = $field . '_completion';
        $dir = $this->indexDir . $this->IndexName . '/index/' . $field . '.db';
        $pdo = new PDO_sqlite($dir);
        $res = [];
        $sql = "select * from $field where term = '$prefix';";
        $resRow = $pdo->getRow($sql);
        if ($resRow) {
            $list = json_decode($this->systemDecompression($resRow['ids']), true);
            $res['result'] = array_slice($list, 0, $num);
            $res['total'] = count($list);
            $res['limit_num'] = $num;
            $res['return_real_num'] = count($res['result']);
        } else {
            $res['result'] = [];
            $res['total'] = 0;
            $res['limit_num'] = $num;
            $res['return_real_num'] = count($res['result']);
        }
        $this->cache($cacheKey, json_encode($res));
        return $res;
    }

    private function match_fuzzy($query)
    {

        $this->queryList = $query;
        $terms = $query['fc_arr'];
        $prefix = array_map(function ($t) {
            return $t . '*';
        }, $terms);
        $suffex = array_map(function ($t) {
            return '*' . $t;
        }, $terms);
        $terms = array_merge($terms, $prefix, $suffex);
        $query['text'] = $terms;
        $res = $this->match_prefix_suffix($query);
        return $res;
    }

    public function search($query)
    {
        if (!$this->checkIndex($this->IndexName)) {
            $this->throwWindException($this->IndexName . ' 索引库不存在', 0);
        }
        if (isset($query['aggs_group']) || isset($query['aggs_group_simp'])  || isset($query['aggs_metrics'])) {
            $res = $this->aggsSearch($query);
        } else if (isset($query['match_geo'])) {
            $res = $this->geoDistanceSearch($query);
        } else if (isset($query['match_auto_completion'])) {
            $res = $this->autoCompletionSearch($query);
        } else {
            $res = $this->normalSearch($query);
        }
        return $res;
    }
}
