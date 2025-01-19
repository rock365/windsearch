# windsearch

windsearch，专业的PHP全文检索解决方案，无需安装，引入即可使用。功能强大，包含多种检索模式。

支持的搜索模式：

**match 单字段检索**

**multi_match 多字段检索**

**match_prefix 前缀匹配**

**match_phrase 短语搜索**

**match_terms 精确搜索**

**match_bool 布尔搜索(无限嵌套)**

**match_range 范围查询**

**match_all 查询所有数据**

**match_rand 随机获取数据**

**match_primarykey 根据主键查询数据**

**match_geo 地理空间搜索**

**match_auto_completion 自动补全**



### 多字段检索语法示例

```php
// 搜索多个字段
$query = [
    'multi_match' => [
        'field' => [
            [
                'name' => 'title',
                'query' => $query,//搜索内容
                'analyzer' => 'segment', // 词典分词模式 complete/not,代表整体匹配; segment 中文分词; 
                // 'analyzer' => [ //ngram分词模式 分词长度为2个字符
                //     'ngram' => 2,
                //     // 'separator' => ',',//分隔符分词模式
                // ],
                'highlight' => [ // 结果高亮
                    'is_cut' => 'cut', // 是否将未匹配的地方截取掉
                    'fixed_length' => '', // 保留多少字符，若为空，则保持原长度
                ],
                // 'weight' => 1,//字段权重
                'minimum_should_match' => '75%', //控制精度 最少应该匹配几个,值可以为整型或百分比
            ],
            [
                'name' => 'tags',
                'query' => '蔬菜',
                'analyzer' => 'segment', // 分词模式 complete/not,代表不分词 整体匹配; segment 中文分词; 
                'highlight' => [ //结果高亮
                    'is_cut' => 'not_cut', // 是否将未匹配的地方截取掉
                    'fixed_length' => '', // 保留多少字符，若为空，则保持原长度
                ],
                // 'weight' => 2,//字段权重
                // 'minimum_should_match' => '15%',//控制精度 最少应该匹配几个，值可以为整型或百分比

            ]

        ],
        // 字段间的匹配模式
        'field_operator'=>'and', // and 同时满足； or 满足其中一个即可
        // 结果排序 为空时，自动按命中个数倒排
        'sort' => [
            // 'time' => 'asc' //asc 按字段值正序 desc 按字段值倒序
        ],
        'list_rows' => $listRows, // 每页多少条数据
        'page' => $page, // 第几页

    ]
];

// 搜索接口
$res = $Wind->search($query);
// $res 返回的最终结果，可直接渲染到前台页面
// ...

```

### 地理空间检索语法示例

```php
// 地理空间搜索
$query = [
    'match_geo' => [
        'field' => [
            'name' => 'location', // 要进行搜索的字段，类型必须为geo_point
            'geo_point' => [
                'lat' => '30.621900130545825', //纬度
                'lon' => '114.28891807367313', //经度
            ],
            // 'distance' => '10000m', // 10公里范围内
            'distance' => ['10km', '20km'], // 1公里到10公里范围内
        ],
        // 结果排序
        'sort' => [
            'geo_distance' => 'asc' //asc 按距离升序 desc 按距离降序
        ],
        '_source' => ['title', 'descr'],//自定义返回字段
        'list_rows' => $listRows, //每页多少条数据
        'page' => $page, //第几页
    ]
];

// 搜索接口
$res = $Wind->search($query);
// $res 返回的最终结果，可直接渲染到前台页面
// ...
```

### 搜索过滤语法示例

搜索可以添加强大的filter**过滤功能**，可以过滤geo距离，数值，关键词，日期。

```php
// 搜索单个字段
$query = [
    'match' => [
        'field' => [
           // 此处为搜索字段信息...
        ],
        
        //筛选过滤
        'filter' => [
            'conditions' => [
                //地理位置过滤
                'location' => [
                    // 中心经纬度,支持字符串、数组
                    // 字符串：纬度,经度   数组：[纬度,经度]
                    // '30.621900130545825,114.28891807367313'
                    'geo_point' => [
                        'lat' => '30.621900130545825', //纬度
                        'lon' => '114.28891807367313', //经度
                    ],
                    'distance' => '8000m', // 8公里范围内
                   // 'distance' => ['1km', '10km'], // 1公里到10公里范围内
                ],
                
                // 数值过滤
                'score' => [
                    // 大于等于 小于
                    'gte' => 20,
                    'lt' => 40,
                    
					// 等于
                    // 'eq' => 25,
                    
                    // 不等于
                    // 'noteq'=>15,
                    
					// 存在于
                    // 'in'=>[13,25],
                    
					// 不存在于
                    // 'notin'=>[13,25],
					
                ],
                
                // 主键过滤
                'uid'=>[
                    'in'=>[
                        '2E1ED673-5F06-A398-C556-10C35CFAAA10',
                        //...
                    ],
                    'notin'=>[
                        '2E1ED673-5F06-A398-C556-10C35CFAAA10',
                         //...
                    ],
                ],
                
                // 时间过滤
                'time' => [
                    // 支持时间戳、字符串
                    'gte' => '2024-03-10',
					'lt' => '2024-03-15',
                ],
                
                //字符串过滤
                'area'=>[
                    // 字符串匹配 限制keyword，text字段
                    // 完全匹配、前缀匹配
                    // 支持字符串、数组
                    // 'match' =>'中国',
                    // 'match' =>['中国','澳大利亚'],
                    // 完全匹配跟前缀匹配,可混合设置
                    'match' => ['中国','澳大*']
                ]

            ],
        ],
        
        
		// 结果排序 为空时，自动按命中个数倒排
        'sort' => [
            'time' => 'desc' //asc 按字段值正序 desc 按字段值倒序
        ],
        'list_rows' => $listRows, //每页多少条数据
        'page' => $page, //第几页

    ]

];

// 搜索接口
$res = $Wind->search($query, $page, $listRows);
// 返回的最终结果，可直接渲染到前台页面
$resArr = $res['result'];
```



检索引擎的使用不应该占据开发人员太多时间，windsearch**所有的操作都已经接口化**，照着文档填写调用即可，简单到不能再简单。引擎内部包含多种数据结构跟算法，但你无需关注任何东西，直接使用它就行。

所有的搜索语法跟ES大差不差，但更清晰简洁。对网站配置几乎没有什么要求（毕竟内存的成本还是挺高的），无需配置任何东西，导入即可使用，无需维护，能跟PHP项目完美融合。



**支持自定义分词插件；**

**支持精细化搜索配置；**（结果高亮、自定义返回字段、字段权重、minimum_should_match、结果按字段值排序、operator等）

**支持生成增量索引并合并到主索引；**

**支持搜索结果filter过滤；**

**支持搜索结果按字段值排序；**（按某个字段值排序、按距离排序（如果存在经纬度字段））

------

商业需求请联系：

vx：azg555666



当然，如果你只是想为博客网站添加一个简单的模糊搜索功能，没有其它复杂的需求，你可以使用免费的 菜狗搜索： [https://github.com/rock365/caigou](https://github.com/rock365/caigou)
