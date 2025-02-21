<?php

namespace WindSearch\Core;

use WindSearch\Exceptions\WindException;


class Geohash
{
    private static $base32 = '0123456789bcdefghjkmnpqrstuvwxyz';

    /**
     * 经纬度编码
     */
    public static function encode($lat, $lon, $precision = null)
    {
        $lat = (float)$lat;
        $lon = (float)$lon;

        $latMin = -90.0;
        $latMax = 90.0;
        $lonMin = -180.0;
        $lonMax = 180.0;
        $precisionMin = 1;
        $precisionMax = 12;

        if ($lat < $latMin || $lat > $latMax) {
            throw new WindException('纬度范围在-90到90之间', 0);
        }

        if ($lon < $lonMin || $lon > $lonMax) {
            throw new WindException('经度范围在-180到180之间', 0);
        }

        if (($precision !== null) && ($precision < $precisionMin || $precision > $precisionMax)) {
            throw new WindException('精度必须在1到12之间', 0);
        }

        // 如果没有设置精确度，则会不停逼近，直到与原始经纬度相同为止
        // 如果有精度参数，则只会逼近固定次数
        if ($precision === null) {
            // 得到经纬度小数位的长度
            $latDecimals = self::numberOfDecimals($lat);
            $lonDecimals = self::numberOfDecimals($lon);
            // 精度由低到高，使生成的值不断逼近输入经纬度的值
            foreach (range(1, 12) as $targetPrecision) {
                $hash = self::encode($lat, $lon, $targetPrecision);
                // 解码 用于跟原始经纬度比对
                $position = self::decode($hash);

                $latPosition = $position['lat'];
                $lonPosition = $position['lon'];
                // 保留固定小数位
                $latPosition = (float) number_format($latPosition, $latDecimals);
                $lonPosition = (float) number_format($lonPosition, $lonDecimals);
                // 直到精度与输入的值完全相同，返回编码结果
                if ($lat === $latPosition && $lon === $lonPosition) {
                    return $hash;
                }

                $precision = 12; // 精度
            }
        }

        $idx = 0; // 它的值对应着base32字符串的固定位置，用于构成geohash字符串
        $bit = 0; // 逼近次数，每逼近5次，生成1位（geohash由多个位构成）
        $evenBit = true; //状态用于切换处理
        $geohash = ''; //最终的hash字符串
        $geohashLength = 0; //最终的geohash字符串的长度
        // 逼近操作
        while ($geohashLength < $precision) {
            if ($evenBit) {
                // 平分 东-西 经度
                $lonMid = ($lonMin + $lonMax) / 2;

                if ($lon >= $lonMid) {
                    $idx = ($idx * 2) + 1;
                    $lonMin = $lonMid;
                } else {
                    $idx *= 2;
                    $lonMax = $lonMid;
                }
            } else {
                // 平分 南-北 纬度
                $latMid = ($latMin + $latMax) / 2;

                if ($lat >= $latMid) {
                    $idx = $idx * 2 + 1;
                    $latMin = $latMid;
                } else {
                    $idx *= 2;
                    $latMax = $latMid;
                }
            }

            $evenBit = !$evenBit;


            if (++$bit === 5) {
                // 每5位，生成一个字符
                $geohash .= self::$base32[$idx];
                // geohash字符串长度加1
                $geohashLength++;
                $bit = 0;
                $idx = 0;
            }
        }

        return $geohash;
    }


    public static function encodeLatLon($lat, $lon)
    {

        $latMin = -90.0;
        $latMax = 90.0;
        $lonMin = -180.0;
        $lonMax = 180.0;

        $precision = 12; //精度 12位编码
        $idx = 0; // 它的值对应着base32字符串的固定位置，用于构成geohash字符串
        $bit = 0; // 逼近次数，每逼近5次，生成1位（geohash由多个位构成）
        $evenBit = true; //状态用于切换处理
        $geohash = ''; //最终的hash字符串
        $geohashLength = 0; //最终的geohash字符串的长度
        // 逼近操作
        while ($geohashLength < $precision) {
            if ($evenBit) {
                // 平分 东-西 经度
                $lonMid = ($lonMin + $lonMax) / 2;

                if ($lon >= $lonMid) {

                    $idx = ($idx * 2) + 1;
                    $lonMin = $lonMid;
                } else {
                    $idx *= 2;
                    $lonMax = $lonMid;
                }
            } else {
                // 平分 南-北 纬度
                $latMid = ($latMin + $latMax) / 2;

                if ($lat >= $latMid) {
                    $idx = $idx * 2 + 1;
                    $latMin = $latMid;
                } else {
                    $idx *= 2;
                    $latMax = $latMid;
                }
            }

            $evenBit = !$evenBit;


            if (++$bit === 5) {
                // 每5位，生成一个字符
                $geohash .= self::$base32[$idx];

                // geohash字符串长度加1
                $geohashLength++;
                $bit = 0;
                $idx = 0;
            }
        }

        return $geohash;
    }


    public static function decode($geohash)
    {
        $bounds = self::bounds($geohash);

        $latMin = $bounds['sw']['lat'];
        $lonMin = $bounds['sw']['lon'];
        $latMax = $bounds['ne']['lat'];
        $lonMax = $bounds['ne']['lon'];

        // cell centre
        $lat = ($latMin + $latMax) / 2;
        $lon = ($lonMin + $lonMax) / 2;

        // round to close to centre without excessive precision: ⌊2-log10(Δ°)⌋ decimal places
        $latPrecision = floor(2 - log10($latMax - $latMin));
        $lonPrecision = floor(2 - log10($lonMax - $lonMin));

        $latString = number_format($lat, (int) $latPrecision);
        $lonString = number_format($lon, (int) $lonPrecision);

        return [
            'lat' => (float) $latString,
            'lon' => (float) $lonString,
        ];
    }


    public static function bounds($geohash)
    {
        $geohash = strtolower($geohash);

        $evenBit = true;
        $latMin = -90.0;
        $latMax = 90.0;
        $lonMin = -180.0;
        $lonMax = 180.0;

        $geohashLength = strlen($geohash);

        for ($i = 0; $i < $geohashLength; $i++) {
            $char = $geohash[$i];
            $idx = strpos(self::$base32, $char);

            for ($n = 4; $n >= 0; $n--) {
                $bitN = $idx >> $n & 1;

                if ($evenBit) {
                    // longitude
                    $lonMid = ($lonMin + $lonMax) / 2;
                    if ($bitN === 1) {
                        $lonMin = $lonMid;
                    } else {
                        $lonMax = $lonMid;
                    }
                } else {
                    // latitude
                    $latMid = ($latMin + $latMax) / 2;

                    if ($bitN === 1) {
                        $latMin = $latMid;
                    } else {
                        $latMax = $latMid;
                    }
                }

                $evenBit = !$evenBit;
            }
        }

        return [
            'sw' => ['lat' => $latMin, 'lon' => $lonMin],
            'ne' => ['lat' => $latMax, 'lon' => $lonMax],
        ];
    }

    public static function adjacent($geohash, $direction)
    {
        $geohash = strtolower($geohash);
        $direction = strtolower($direction);

        $neighbor = [
            'n' => ['p0r21436x8zb9dcf5h7kjnmqesgutwvy', 'bc01fg45238967deuvhjyznpkmstqrwx'],
            's' => ['14365h7k9dcfesgujnmqp0r2twvyx8zb', '238967debc01fg45kmstqrwxuvhjyznp'],
            'e' => ['bc01fg45238967deuvhjyznpkmstqrwx', 'p0r21436x8zb9dcf5h7kjnmqesgutwvy'],
            'w' => ['238967debc01fg45kmstqrwxuvhjyznp', '14365h7k9dcfesgujnmqp0r2twvyx8zb'],
        ];

        $border = [
            'n' =>  ['prxz',     'bcfguvyz'],
            's' =>  ['028b',     '0145hjnp'],
            'e' =>  ['bcfguvyz', 'prxz'],
            'w' =>  ['0145hjnp', '028b'],
        ];

        $lastChar = substr($geohash, -1);
        $parent = substr($geohash, 0, -1);

        $type = strlen($geohash) % 2;

        // 检查不共享公共前缀的边缘情况
        if ($parent !== '' && strpos($border[$direction][$type], $lastChar) !== false) {
            $parent = self::adjacent($parent, $direction);
        }

        return $parent . self::$base32[strpos($neighbor[$direction][$type], $lastChar)];
    }


    public static function neighbors($geohash)
    {
        $n = self::adjacent($geohash, 'n');
        $s = self::adjacent($geohash, 's');

        return [
            'n' => $n,
            'ne' => self::adjacent($n, 'e'),
            'e' => self::adjacent($geohash, 'e'),
            'se' => self::adjacent($s, 'e'),
            's' => $s,
            'sw' => self::adjacent($s, 'w'),
            'w' => self::adjacent($geohash, 'w'),
            'nw' => self::adjacent($n, 'w'),
        ];
    }

    /**
     * 返回小数位的长度
     */
    private static function numberOfDecimals($value)
    {
        // 保留14位小数 位数四舍五入
        $string = number_format($value, 14);
        // 小数点的位置
        $point = strpos($string, '.');

        // 截取小数点后面的数字
        $decimals = substr($string, $point + 1);

        // 删除右边的 0
        $decimals = rtrim($decimals, '0');
        // 返回剩下内容的长度
        return strlen($decimals);
    }




    /**
     * 判断点是否在多边形内部（使用射线法）
     *
     * @param array $polygon 多边形顶点数组，每个顶点是一个包含纬度和经度的数组
     * @param float $pointLat 待判断点的纬度
     * @param float $pointLon 待判断点的经度
     * @return bool 是否在多边形内部
     */
    public static function isPointInPolygon($polygon, $pointLat, $pointLon)
    {
        $numVertices = count($polygon);
        $j = $numVertices - 1; // 多边形的最后一个顶点的索引
        $oddNodes = false; // 用于记录射线与多边形边界的交点数量的奇偶性

        for ($i = 0; $i < $numVertices; $i++) {
            $vertexLat = $polygon[$i][0]; // 当前顶点的纬度
            $vertexLon = $polygon[$i][1]; // 当前顶点的经度
            $nextVertexLat = $polygon[$j][0]; // 下一个顶点的纬度
            $nextVertexLon = $polygon[$j][1]; // 下一个顶点的经度

            // 检查射线是否与当前边相交
            if (($pointLon < max($vertexLon, $nextVertexLon)) &&
                ($pointLon > min($vertexLon, $nextVertexLon)) &&
                ($pointLat < ($nextVertexLat - $vertexLat) * ($pointLon - $vertexLon) / ($nextVertexLon - $vertexLon) + $vertexLat)
            ) {
                $oddNodes = !$oddNodes; // 如果相交，则改变奇偶性
            }

            $j = $i; // 移动到下一个顶点
        }

        return $oddNodes; // 如果交点数量为奇数，则点在多边形内部
    }



    /************************************判断多边形相交*************************************** */

    /**
     * 判断线段是否与多边形相交
     *
     * @param array $polygon 多边形顶点数组，每个顶点是一个包含纬度和经度的数组
     * @param array $line 线段的两个端点，每个端点是一个包含纬度和经度的数组
     * @return bool 是否相交
     */
    private static function doLineIntersectPolygon($polygon, $line)
    {
        $numVertices = count($polygon);
        for ($i = 0, $j = $numVertices - 1; $i < $numVertices; $j = $i++) {
            if (self::doLineIntersectLine($polygon[$i], $polygon[$j], $line[0], $line[1])) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断两条线段是否相交
     *
     * @param array $p1 线段1的第一个端点（纬度，经度）
     * @param array $p2 线段1的第二个端点（纬度，经度）
     * @param array $p3 线段2的第一个端点（纬度，经度）
     * @param array $p4 线段2的第二个端点（纬度，经度）
     * @return bool 是否相交
     */
    private static function doLineIntersectLine($p1, $p2, $p3, $p4)
    {
        // 计算方向
        $o1 = self::orientation($p1, $p2, $p3);
        $o2 = self::orientation($p1, $p2, $p4);
        $o3 = self::orientation($p3, $p4, $p1);
        $o4 = self::orientation($p3, $p4, $p2);

        // 一般情况
        if ($o1 != $o2 && $o3 != $o4) {
            return true;
        }

        // 特殊情况：共线但不相交
        if ($o1 == 0 && self::onSegment($p1, $p3, $p2)) return true;
        if ($o2 == 0 && self::onSegment($p1, $p4, $p2)) return true;
        if ($o3 == 0 && self::onSegment($p3, $p1, $p4)) return true;
        if ($o4 == 0 && self::onSegment($p3, $p2, $p4)) return true;

        return false;
    }

    /**
     * 计算三点的方向
     *
     * @param array $p 起点（纬度，经度）
     * @param array $q 中间点（纬度，经度）
     * @param array $r 终点（纬度，经度）
     * @return int 方向：0表示共线，-1表示顺时针，1表示逆时针
     */
    private static function orientation($p, $q, $r)
    {
        $val = ($q[0] - $p[0]) * ($r[1] - $q[1]) - ($q[1] - $p[1]) * ($r[0] - $q[0]);
        if ($val == 0) return 0; // 共线
        return ($val > 0) ? 1 : 2; // 顺时针或逆时针，这里简化为1和2，实际代码中可以用-1和1表示
    }

    /**
     * 判断点是否在线段上
     *
     * @param array $p 线段的起点
     * @param array $q 线段的终点
     * @param array $r 待判断的点
     * @return bool 是否在线段上
     */
    private static function onSegment($p, $q, $r)
    {
        return $q[0] >= min($p[0], $r[0]) && $q[0] <= max($p[0], $r[0]) &&
            $q[1] >= min($p[1], $r[1]) && $q[1] <= max($p[1], $r[1]);
    }

    /**
     * 判断两个多边形是否存在相交
     *
     * @param array $polygon1 第一个多边形的顶点数组
     * @param array $polygon2 第二个多边形的顶点数组
     * @return bool 是否相交
     */
    public static function doPolygonsIntersect($polygon1, $polygon2)
    {
        // 检查polygon1的每条边是否与polygon2相交
        $numEdges1 = count($polygon1);
        for ($i = 0; $i < $numEdges1; $i++) {
            $edge = [$polygon1[$i], $polygon1[($i + 1) % $numEdges1]];
            if (self::doLineIntersectPolygon($polygon2, $edge)) {
                return true;
            }
        }

        // 检查polygon2的每条边是否与polygon1相交（理论上这一步是多余的，因为相交是对称的，但为了代码的清晰性还是加上）
        // 在实际应用中，可以根据需要省略这一步
        $numEdges2 = count($polygon2);
        for ($i = 0; $i < $numEdges2; $i++) {
            $edge = [$polygon2[$i], $polygon2[($i + 1) % $numEdges2]];
            // 注意：这里实际上不需要再次检查与polygon1的相交，因为上面的循环已经做过了
            // 这里只是为了展示如何遍历polygon2的边，实际代码中应该省略这个循环或将其注释掉
        }

        // 如果没有发现相交，则返回false
        return false;
    }
}
