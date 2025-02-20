<?php

namespace WindSearch\Core;

class Faker
{
    private static $firstNameMale = ["John", "Michael", "David", "James", "William"];
    private static $firstNameFemale = ["Mary", "Patricia", "Linda", "Barbara", "Elizabeth"];
    private static $lastName = ["Smith", "Johnson", "Williams", "Brown", "Jones"];
    private static $domains = ["a.com", "b.com", "c.com", "d.com", "e.com", 'f.com', 'g.com', 'h.com', 'i.com', 'j.com', 'k.com', 'l.com', 'm.com'];

    // 生成随机男性名字
    public static function firstNameMale()
    {
        return self::$firstNameMale[array_rand(self::$firstNameMale)];
    }

    // 生成随机女性名字
    public static function firstNameFemale()
    {
        return self::$firstNameFemale[array_rand(self::$firstNameFemale)];
    }

    // 生成随机姓氏
    public static function lastName()
    {
        return self::$lastName[array_rand(self::$lastName)];
    }

    // 生成随机全名（可选性别）
    public static function fullName($gender = null)
    {
        $firstName = ($gender === 'male') ? self::firstNameMale() : (($gender === 'female') ? self::firstNameFemale() : (self::firstNameMale() || self::firstNameFemale()));
        $lastName = self::lastName();
        return "$firstName $lastName";
    }


    private static $nameList = [];
    // 生成随机中文人名
    public static function name($gender = 'mix')
    {
        if ($gender == 'mix') {
            if (!isset(self::$nameList['mix'])) {
                self::$nameList['mix'] = array_filter(explode(PHP_EOL, file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/name.txt')));
            }
        } else if ($gender == 'male') {
            if (!isset(self::$nameList['male'])) {
                self::$nameList['male'] = array_filter(explode(PHP_EOL, file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/name.txt')));
            }
        } else if ($gender == 'female') {
            if (!isset(self::$nameList['female'])) {
                self::$nameList['female'] = array_filter(explode(PHP_EOL, file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/name.txt')));
            }
        }
        return self::$nameList[$gender][array_rand(self::$nameList[$gender])];
    }

    // 生成随机长度数字
    public static function number($len = 10)
    {
        $first = random_int(1, 9);
        $randomNumber = '';
        for ($i = 0; $i < ($len - 1); ++$i) {
            $randomNumber .= random_int(0, 9);
        }
        return $first . $randomNumber;
    }

    // 生成随机手机号
    public static function phoneNumber($len = 11)
    {
        $beg = ['138', '139', '176', '178', '158', '159', '188', '189'];
        $randomNumber = '';
        for ($i = 0; $i < ($len - 3); ++$i) {
            $randomNumber .= random_int(0, 9);
        }
        return $beg[array_rand($beg)] . $randomNumber;
    }

    // 生成随机电子邮件
    public static function email()
    {
        // $firstName = self::firstNameMale() || self::firstNameFemale();
        // $lastName = self::lastName();
        // $domain = self::$domains[array_rand(self::$domains)];
        // return strtolower("$firstName.$lastName@$domain");

        $domain = self::$domains[array_rand(self::$domains)];
        return self::number(10) . '@' . $domain;
    }

    // 生成随机地址
    public static function address()
    {
        // $streetNames = ["Main", "Oak", "Pine", "Maple", "Birch"];
        // $streetTypes = ["St", "Ave", "Blvd", "Ln", "Rd"];
        // $cityNames = ["Springfield", "Shelbyville", "Bedrock", "Gotham", "Metropolis"];
        // $states = ["AL", "AK", "AZ", "AR", "CA"]; // 仅作为示例，仅列出几个州

        // $streetName = $streetNames[array_rand($streetNames)];
        // $streetType = $streetTypes[array_rand($streetTypes)];
        // $streetNumber = rand(100, 999);
        // $cityName = $cityNames[array_rand($cityNames)];
        // $state = $states[array_rand($states)];
        // $zipCode = rand(10000, 99999);

        // return "$streetNumber $streetName $streetType, $cityName, $state $zipCode";

        $arr = explode(PHP_EOL, file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/Chinese_character.txt'));
        shuffle($arr);
        $characters = implode('', $arr);
        $len = mb_strlen($characters);
        $res = [];
        for ($i = 0; $i < 6; ++$i) {
            $res[] = mb_substr($characters, rand(0, $len - 10), 2);
        }
        $division = ['省', '市', '县', '镇', '村', '组'];
        $adress = '';
        foreach ($res as $v) {
            $curr = array_shift($division);
            $adress .= $v . $curr . ' ';
        }
        return $adress . rand(100, 999) . '号';
    }

    // 生成随机布尔值
    public static function boolean()
    {
        return rand(0, 1) === 1;
    }

    private static $Chinese_character = [];
    // 生成随机文本
    public static function text($type = 'zh', $length = 100)
    {
        if ($type == 'zh') {
            if (!isset(self::$Chinese_character['zh'])) {
                self::$Chinese_character['zh'] = trim(str_replace(PHP_EOL, '', file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/Chinese_character.txt')));
            }

            $connector = '';
        } else if ($type == 'en') {
            if (!isset(self::$Chinese_character['en'])) {
                self::$Chinese_character['en'] = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            }
            $connector = '';
        } else {
            if (!isset(self::$Chinese_character['zh'])) {
                self::$Chinese_character['zh'] = trim(str_replace(PHP_EOL, '', file_get_contents(__DIR__ . '/../windIndexCore/Chinese_character/Chinese_character.txt')));
            }

            $connector = '';
        }

        $len = mb_strlen(self::$Chinese_character[$type]);
        $text = '';
        for ($i = 0; $i < $length; $i++) {
            $text .= mb_substr(self::$Chinese_character[$type], rand(0, $len - 1), 1);
        }
        $res = '';
        while (strlen($text) > 0) {
            $cutlen = rand(3, 7);
            $res .= $connector . mb_substr($text, 0, $cutlen);
            $text = mb_substr($text, $cutlen);
        }
        $res = trim($res);
        $res = trim($res, ',');
        return $res;
    }

    // 生成随机uuid
    public static function uuid()
    {

        //根据当前时间（微秒计）生成唯一id
        $charid = strtoupper(md5(uniqid(rand(), true)));
        // 连接符
        $hyphen = '-';

        $uuid = substr($charid, 0, 8) . $hyphen . substr($charid, 8, 4) . $hyphen . substr($charid, 12, 4) . $hyphen . substr($charid, 16, 4) . $hyphen . substr($charid, 20, 12);

        return $uuid;
    }

    // 生成随机日期
    public static function date($start = '1970-01-01', $end = '')
    {
        // 定义起始日期和结束日期
        $startDate = $start;
        $endDate = ($end == '') ? date('Y-m-d', time()) : $end;

        // 将起始日期和结束日期转换为时间戳
        $startTimestamp = strtotime($startDate);
        $endTimestamp = strtotime($endDate);

        // 生成一个位于起始时间戳和结束时间戳之间的随机时间戳
        $randomTimestamp = $startTimestamp + rand(0, $endTimestamp - $startTimestamp);

        // 将随机时间戳格式化为日期
        $randomDate = date('Y-m-d', $randomTimestamp);

        return $randomDate;
    }


    private static function getRandomHexColor($includeAlpha = false)
    {
        $hex = '#';
        if ($includeAlpha) {
            // 8位十六进制，包括alpha通道
            $hex .= dechex(rand(0, 0xFFFFFFFF) & 0xFFFFFFFF);
            // 确保长度是8位（可能需要填充前导零）
            $hex = substr($hex, 0, 9); // 保留#号和一个字符，然后截取到8位
        } else {
            // 6位十六进制
            $hex .= dechex(rand(0, 0xFFFFFF) & 0xFFFFFF);
            // 确保长度是6位（可能需要填充前导零）
            $hex = substr($hex, 0, 7); // 保留#号和六个字符
        }
        return $hex;
    }

    private static function getRandomRGBColor()
    {
        return sprintf('rgb(%d, %d, %d)', rand(0, 255), rand(0, 255), rand(0, 255));
    }

    private static function getRandomRGBAColor()
    {
        $alpha = rand(0, 100) / 100.0; // 生成0到1之间的浮点数
        return sprintf('rgba(%d, %d, %d, %.2f)', rand(0, 255), rand(0, 255), rand(0, 255), $alpha);
    }
    private static function getRandomHSLColor()
    {
        $hue = rand(0, 360);         // 色调：0到360度
        $saturation = rand(0, 100);  // 饱和度：0%到100%
        $lightness = rand(0, 100);   // 亮度：0%到100%
        return sprintf('hsl(%d, %d, %d)', $hue, $saturation, $lightness);
    }
    // 生成随机颜色值
    public static function color($type = 'hex')
    {

        if ($type == 'hex') {
            return self::getRandomHexColor(); // 例如: #3e2f1b true:#3e2f1b22
        } else if ($type == 'rgb') {
            return self::getRandomRGBColor();
        } else if ($type == 'rgba') {
            return self::getRandomRGBAColor();
        } else if ($type == 'hsl') {
            return self::getRandomHSLColor();
        } else {
            return self::getRandomHexColor(); // 例如: #3e2f1b true:#3e2f1b22
        }
    }

    // 生成随机工号
    public static function employeeID($length = 10, $prefix = '')
    {

        $len = $length - mb_strlen($prefix);
        return $prefix . self::number($len);
        // 生成随机字节
        $randomBytes = random_bytes(ceil($length / 2));
        // 转换为十六进制字符串
        $hexString = bin2hex($randomBytes);
        // 截取指定长度的字符串
        $employeeID = substr($hexString, 0, $length);

        // 如果长度不足，则继续补充随机字符（可选）
        if (strlen($employeeID) < $length) {
            $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
            $charactersLength = strlen($characters);
            for ($i = strlen($employeeID); $i < $length; $i++) {
                $employeeID .= $characters[rand(0, $charactersLength - 1)];
            }
        }

        // 返回生成的唯一工号
        return $employeeID;
    }


    private static function generateRandomIPv4()
    {
        return implode('.', array_map(function () {
            return rand(0, 255);
        }, range(1, 4)));
    }

    private static function generateRandomIPv6()
    {
        $hexChars = '0123456789abcdef';
        $ipv6 = [];
        for ($i = 0; $i < 8; $i++) {
            $segment = '';
            for ($j = 0; $j < 4; $j++) {
                $segment .= $hexChars[rand(0, 15)];
            }
            $ipv6[] = $segment;
        }
        return implode(':', $ipv6);
    }

    // 生成随机ip
    public static function ip($type = 'ipv4')
    {
        if ($type == 'ipv4') {
            return self::generateRandomIPv4();
        } else if ($type == 'ipv6') {
            return self::generateRandomIPv6();
        } else {
            return self::generateRandomIPv4();
        }
    }


    // 生成随机用户名
    public static function userName($type = 'zh', $length = 6)
    {
        $rep = [
            ' ' => '',
            ',' => '',
        ];
        return strtr(self::text($type, $length), $rep);
    }

    // 生成随机公司名称
    public static function companyName()
    {
        // 定义地点
        $locations = [
            "哈尔滨", // 黑龙江省省会
            "长春",   // 吉林省省会
            "沈阳",   // 辽宁省省会
            "石家庄", // 河北省省会
            "郑州",   // 河南省省会
            "济南",   // 山东省省会
            "太原",   // 山西省省会
            "合肥",   // 安徽省省会
            "武汉",   // 湖北省省会
            "长沙",   // 湖南省省会
            "南京",   // 江苏省省会
            "成都",   // 四川省省会
            "贵阳",   // 贵州省省会
            "昆明",   // 云南省省会
            "西安",   // 陕西省省会
            "兰州",   // 甘肃省省会
            "南昌",   // 江西省省会
            "杭州",   // 浙江省省会
            "广州",   // 广东省省会
            "福州",   // 福建省省会
            "海口",   // 海南省省会
            "南宁",   // 广西壮族自治区首府
            "呼和浩特", // 内蒙古自治区首府
            "拉萨",   // 西藏自治区首府
            "银川",   // 宁夏回族自治区首府
            "乌鲁木齐", // 新疆维吾尔自治区首府
            "西宁",   // 青海省省会
            "台北",   // 台湾省省会
            "北京"    // 首都
        ];
        // 名称
        $adjectives = [
            "创新",
            "智慧",
            "科技",
            "金融",
            "教育",
            "健康",
            "环保",
            "能源",
            "信息",
            "传媒",
            "网络",
            "国际",
            "投资",
            "发展",
            "管理",
            "咨询",
            "服务",
            "制造",
            "建设",
            "设计",
            "软件",
            "硬件",
            "生物",
            "文化",
            "旅游",
            "物流",
            "电子",
            "商务",
            "农业",
            "食品",
            "医药",
            "汽车",
            "通信",
            "互联网",
            "大数据",
            "云计算",
            "人工智能",
            "物联网",
            "区块链",
            "新能源",
            "新材料",
            "智能制造",
            "智慧城市",
            "绿色",
            "数字"
        ];
        // 后缀
        $suffixs = ['集团', '有限公司', '公司', '集团股份有限公司', '控股有限公司'];

        // 从每个列表中随机选择一个或多个词汇（这里每个列表只选一个）
        $adjective = $adjectives[array_rand($adjectives)];

        $location = $locations[array_rand($locations)];
        $suffix = $suffixs[array_rand($suffixs)];
        // 组合成公司名称（这里不加入地点名词，如果需要可以加上）
        $companyName = $location . $adjective . $suffix;
        // 如果需要地点名词，可以这样组合：$companyName = $location . $adjective . $noun;

        // 返回生成的随机公司名称
        return $companyName;
    }
}
