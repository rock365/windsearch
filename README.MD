### Windsearch

windsearch，专业的PHP全文检索解决方案，可承受百万至千万的数据。**零安装配置**、零心智负担，拥有**强大的算法**，包含**多种检索模式**，集成到PHP项目后，**无需维护，一劳永逸**。

本搜索引擎适合外贸独立站、垂直社区、区域性电商、cms系统、PHP开发工作室、服务器配置一般的网站、需要快速搭建搜索的网站、个人博客等。

在线文档：[https://rock365.github.io/](https://rock365.github.io/) 偶尔无法访问，多刷新几次就行

**WindSearch采用双重授权模式：**

1. 根据AGPLv3协议**免费使用**，需遵守其全部条款

2. 或通过支付授权费获得商业许可，免除AGPL限制



### 它的优势是什么？

**易用性**：完全零配置（你没有听错），没有任何繁杂臃肿的安装设置，一行代码就能搭建完成；完全接口化操作；文档详细，复制粘贴就能实现各种搜索场景。

**安全**：完全本地化部署，所有代码完全有您掌握；

**PHP原生友好**：windsearch是纯PHP开发的，能跟PHP项目无缝集成，不会产生跨语言调用的性能损耗；

**零运维成本**：不会产生任何维护调优的成本，只要网站正常运行，搜索引擎就能一直运行，永不宕机；

**性能强悍**：百万级数据毫秒级响应，千万级数据面不改色，且拥有多种搜索模式；

**极高性价比**：磁盘式搜索引擎，对服务器内存资源占用极小，可大幅降低服务器成本；



### 多种搜索模式

**match 单字段检索**

**multi_match 多字段检索**

**match_fuzzy 模糊搜索**

**match_prefix 前缀匹配**

**match_suffix 后缀匹配**

**match_prefix_suffix 前后缀混合匹配**

**match_phrase 短语搜索**

**match_terms 精确搜索**

**match_bool 布尔搜索(无限嵌套)**

**match_range 范围查询**

**match_all 查询所有数据**

**match_rand 随机获取数据**

**match_primarykey 根据主键查询数据**

**match_geo 地理空间搜索**

**match_auto_completion 自动补全**



### 其它功能

aggs 聚合查询：分组聚合、指标聚合、分组嵌套聚合；

搜索统计：可对前台用户的搜索数据进行保存统计，可按年、按月、按日来查看搜索词语的频率统计数据；

敏感词检测：包括一键导入所有敏感词，检测字符串是否存在敏感词、检测并替换敏感词、返回所有包含的敏感词；



### 亮点介绍

**操作完全接口化；**

**原生支持中文分词；**

**支持配置同义词；**

**支持自定义分词插件；**

**支持精细化搜索设置；**（结果高亮、自定义返回字段、字段权重、minimum_should_match、结果按字段值排序、数据脱敏返回等）

**支持生成增量索引并合并到主索引；**

**支持实时索引；**

**支持搜索结果filter过滤；**

**支持搜索结果按字段值排序；**（按某个字段值排序、按距离排序（如果存在经纬度字段））

**更合理的排序：**

综合排序时，命中的词语之间越紧密，排序越靠前，例如：

1，xxxx明天xxxx会xxxxxxx更好

2，xxx明天会更好xxxxxxx

排序后，2会在1前面

**支持int递增主键、uuid主键；**



### 快速安装使用

##### 安装导入WindSearch

**环境要求：**

* UTF-8编码
* PHP ≥7.3
* mbstring Extension
* PDO Extension
* SQLite Extension

**开始安装：**
将WindSearch下载到本地你喜欢的文件夹，这是一个纯PHP的全文检索引擎

再引入入口文件，注意具体文件路径

```php
require_once 'yourdirname/vendor/autoload.php';
```
至此，安装已经全部完成，再无其它任何配置，快不快？

##### 建索引库

复制修改粘贴即可，跟mysql建表差不多

```php
$mapping = [
  	//设置索引库的名称，比如对应的表名
    'name' => 'test', 
    // 字段配置
    'field' => [ 
        [
            'name' => 'id',// 主键名称 主键必须设置
            'type' => 'primarykey', //数据类型为主键 必须设置
            'primarykey_type' => 'Int_Incremental', // int递增
        ],
        [
            'name' => 'title',
            'index' => true,
            'type' => 'text',
            'analyzer' => 'segment',
        ],
        [
            'name' => 'tags',
            'index' => true,
            'type' => 'keyword', 
        ]
        [
            'name' => 'score',
            'type' => 'numeric', 
        ],
        [
            'name' => 'time',
            'type' => 'date'
        ],

        [
            'name' => 'descr',
			'type' => 'text',
        ],

    ]

];

// 实例化对象
$Wind = new \WindSearch\Index\Wind('test'); //$indexName 当前索引库的名称
//检查是否存在此索引库
$is_index = $Wind->checkIndex();
// 如果存在此索引库
if ($is_index) {
    //删除索引库
    $Wind->delIndex();
}
//创建索引库
$Wind->createIndex($mapping);
```
##### 导入数据

```php
//实例化引擎
$Wind = new \WindSearch\Index\Wind('test');
// 初始化
$Wind->buildIndexInit();
// 开启分词，导入数据时，加true可加快速度
$Wind->loadAnalyzer(true);

// 数据量小（内容少于一万条），则可以一次性全部导入
// selectAll...
// $result：一次性查询的所有内容
foreach ($result as $v) {
    $Wind->indexer($v);
}
// 批量写入文件保存
$Wind->batchWrite();
```
##### 构建索引

```php
// 数据导入结束后，接着可立即调用此方法构建索引
// 注意，数据量大时，此步骤会比较耗时
$Wind->buildIndex();
```

##### 开始搜索

```php
//实例化引擎
$Wind = new \WindSearch\Index\Wind('test');

//开启分词功能
$Wind->loadAnalyzer();

//开始搜索

// 搜索单个字段
$query = [
    'match' => [
        'field' => [
            'name' => 'title',
            'query' => $text,
        ],
        'list_rows' => $listRows, //每页多少条数据
        'page' => $page, //第几页

    ]

];

// 搜索接口
$res = $Wind->search($query, $page, $listRows);
// 返回的最终结果，可直接渲染到前台页面
$resArr = $res['result']['_source'];
```
### 结语

以上流程可以快速实现一个PHP全文检索，当然，这些只是餐前甜点，WindSearch还有更深入、更丰富的搜索功能等你挖掘：
在线开发文档：[https://rock365.github.io/](https://rock365.github.io/) 偶尔访问不稳定，多刷新几次即可

点个star吧亲亲O(∩_∩)O~~谢谢大家！



微信：azg555666

![](https://github.com/rock365/img/blob/main/afe22e05ee161083cfbd1336f7facd2.jpg)


