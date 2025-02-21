# WindSearch 2.0 🔍 [![PHP Version](https://img.shields.io/badge/PHP-7.3%2B-brightgreen)]()

### 让中文搜索变得像呼吸一样简单（新功能开发中...）

[![AGPL License](https://img.shields.io/github/license/rock365/windsearch)](LICENSE) [![GitHub Stars](https://img.shields.io/github/stars/rock365/windsearch?style=social)](https://github.com/rock365/windsearch)



👏 **功能新增**

2.0版本新增：

- [即用模式](https://github.com/rock365/windsearch/blob/main/CHANGELOG.md)
- [Faker数据生成](https://github.com/rock365/windsearch/blob/main/CHANGELOG.md)



🚀 **一行代码开启极速搜索体验！**

使用composer安装：

```bash
composer require rock365/windsearch
```

或 使用Git安装：

```bash
git clone git@github.com:rock365/windsearch.git
```

或 直接github下载。



🚀 **PHP开发者苦ES久矣！**  

还在为Elasticsearch的复杂配置头疼？WindSearch给您：

- **零依赖**：无需Java环境，告别JVM调优噩梦
- **闪电速度**：百万数据毫秒级响应
- **中文优化**：内置20万+专业词库，分词精度超98%
- **内存克星**：小内存也能搜索大数据，内存占用为零



🚀 **众多优势，不容错过！**

- ✅ 比数据库LIKE快**300倍**，比ES轻量**90%**
- ✅ 天然适配**Laravel**/**ThinkPHP**等主流框架
- ✅ 支持「全文搜索」「同义词扩展」「字段权重优化」「自定义分词插件」等
- ✅ 支持增量索引合并
- ✅ 支持实时索引、实时搜索
- ✅ 支持搜索过滤
- ✅ 支持int递增主键、uuid主键



📖 **在线文档**

[https://rock365.github.io/](https://rock365.github.io/) 偶尔无法访问，多刷新几次就行



😁 **多种搜索模式:**

match 单字段检索

multi_match 多字段检索

match_fuzzy 模糊搜索

match_prefix 前缀匹配

match_suffix 后缀匹配

match_prefix_suffix 前后缀混合匹配

match_phrase 短语搜索

match_terms 精确搜索

match_bool 布尔搜索(无限嵌套)

match_range 范围查询

match_all 查询所有数据

match_rand 随机获取数据

match_primarykey 根据主键查询数据

match_geo 地理空间搜索

match_auto_completion 自动补全

aggs 聚合查询：分组聚合、指标聚合、分组嵌套聚合；

搜索统计：可对前台用户的搜索数据进行保存统计，可按年、按月、按日来查看搜索词语的频率统计数据；

敏感词检测：包括一键导入所有敏感词，检测字符串是否存在敏感词、检测并替换敏感词、返回所有包含的敏感词；



🚀 **快速安装使用**

*环境要求：*

* UTF-8编码
* PHP ≥7.3
* mbstring Extension
* PDO Extension
* SQLite Extension



*引入入口文件：*

WindSearch安装完成后，引入入口文件，注意具体文件路径

```php
require_once 'yourdirname/vendor/autoload.php';
```
至此，安装已经全部完成，再无其它任何配置。



*建索引库：*

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
            'index' => true, // 是否索引此字段
            'type' => 'text',
            'analyzer' => 'segment', // 配置分词方式
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
$Wind = new \WindSearch\Index\Wind('test'); //test 当前索引库的名称
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


*导入数据：*

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


*构建索引：*

```php
// 数据导入结束后，接着可立即调用此方法构建索引
// 注意，数据量大时，此步骤会比较耗时
$Wind->buildIndex();
```



*开始搜索：*

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


以上流程可以快速实现一个PHP全文检索，当然，这些只是餐前小菜，WindSearch还有更深入、更丰富的搜索功能等你挖掘：

在线开发文档：[https://rock365.github.io/](https://rock365.github.io/) 偶尔访问不稳定，多刷新几次即可



### 👏 让PHP再次伟大！

点个star吧亲亲O(∩_∩)O~~谢谢大家！

*联系方式：*

微信：azg555666

![](https://github.com/rock365/img/blob/main/afe22e05ee161083cfbd1336f7facd2.jpg)

