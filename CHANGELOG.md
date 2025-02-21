# CHANGELOG [v2.0]

## 功能新增

2.0版本新增“**即用模式**”、“**Faker数据生成”**。

### 即用模式

即用模式适合简单搜索场景，即用模式下，导入、搜索等操作更加简单直接，且无需任何配置。即用模式导入、搜索操作的代码示例：

*导入数据*

```php
// 实例化对象
$Wind = new \WindSearch\Index\Wind('test'); //test 当前索引库的名称
// 清空之前的数据（如果之前使用即用模式导入过数据）
$Wind->deleteFastIndex();
// 批次导入数据
// $res 是从数据库查询的数据
foreach($res as $v){
    $text = $v['title'];
    $primarykey = $v['id'];
    // $text是需要搜索的具体内容，比如title；$primarykey是主键值，比如id的值
	$Wind->fastIndexer($text, $primarykey);
}
//每导入一批数据，就调用此方法进行保存
$Wind->fastBatchWrite();

// 所有数据全部导入完成后，接着构建索引（不一定非得紧接着调用，也可以在其它地方单独调用）
$Wind->fastBuildIndex();

```

*开始搜索*

```php
// 开始搜索
$Wind = new \WindSearch\Index\Wind('test');
// 调用搜索方法
// $page 第几页 $listRows 每页多少条
$res = $Wind->fastSearch($text,$page,$listRows)
// $res：返回的主键（比如id）集合，你可以使用id集合从MySQL等数据库查询原始数据
```



### Faker数据生成

*安装导入*

```php
// 导入代码都是一样的
require_once 'yourdirname/windsearch/vendor/autoload.php';
```

*开始生成*

```php
// 创建一个Faker对象
$faker = \WindSearch\Core\Faker::create();

// 随机生成电子邮件
$email = $faker->email(); // 6099607970@i.com

// 随机生成中文地址（无逻辑）
$address = $faker->address(); // 窖坦省 躁袱市 急陕县 胳披镇 饮藤村 北翌组 859号

// 随机生成布尔值
$boolean = $faker->boolean(); // false

// 随机生成固长度的文本
// 第一个参数支持 zh：中文（无逻辑） en：数字字母，第二个参数代表生成字符长度
$text = $faker->text('zh', 50); // 猖往罢叫阳丹沦颠藉毡

// 随机生成固定长度的数字字符串
$number = $faker->number(20); // 35366395389404570072

// 随机生成中国手机号
$phoneNumber = $faker->phoneNumber(); // 

// 随机生成中文人名
$name = $faker->name(); // 刘爱霞

// 随机生成uuid
$uuid = $faker->uuid(); // 96BE006D-366E-D78A-035D-159205B97B0B

// 随机生成date日期 支持指定起始、结束日期，例如 '2024-01-01','2025-01-01'
$date = $faker->date(); // 2024-08-20

// 随机生成颜色值 参数支持：hex rgb rgba hsl
$color = $faker->color(); // #36a847

// 随机生成工号 第一个参数代表总的长度，第二个参数代表前缀，默认生成随机数字
$employeeID = $faker->employeeID(10, '1101'); // 1101911597

// 随机生成ip地址 参数支持 'ipv4'或'ipv6'，默认'ipv4'
$ip = $faker->ip(); // 235.13.245.196

// 随机生成用户名 参数支持zh：中文，en：数字字母
$userName = $faker->userName('zh'); // 

// 随机生成中文公司名称
$companyName = $faker->companyName(); // 兰州汽车控股有限公司
```



