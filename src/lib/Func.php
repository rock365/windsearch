<?php

namespace WindSearch\Core;


class Func
{

	private $primarykeyType = 'Int_Incremental';

	public function __construct($param)
	{
		$this->primarykeyType = $param;
	}


	/**
	 * 双指针求交集
	 */
	public function doublepointer_intersection($arr1, $arr2)
	{

		// 系统交集函数
		if ($this->primarykeyType == 'UUID') {
			return array_intersect($arr1, $arr2);
		}

		sort($arr1);
		sort($arr2);

		$result = [];
		$count1 = count($arr1);
		$count2 = count($arr2);
		$i = $j = 0;

		while ($i < $count1 && $j < $count2) {
			if ($arr1[$i] < $arr2[$j]) {
				$i++;
			} elseif ($arr1[$i] > $arr2[$j]) {
				$j++;
			} else {
				$result[] = $arr1[$i];
				$i++;
				$j++;
			}
		}

		return $result;
	}

	/**
	 * 双指针求交集 支持多个数组
	 */
	public function multi_doublepointer_intersection($array1 = [])
	{

		// 获取传入的所有参数
		$arrays = func_get_args();

		// 系统交集函数
		if ($this->primarykeyType == 'UUID') {
			return array_intersect(...$arrays);
		}

		// 对每个int数组排序成递增形式
		$arrays = array_map(function ($p) {
			// 升序排列，且索引值从0开始
			sort($p);
			return $p;
		}, $arrays);

		// 踢出第一个数组，并保存，作为比对基础
		$result = array_shift($arrays);
		// 再依次拿出剩下的数组进行比对
		while (!empty($arrays)) {
			$next = array_shift($arrays);

			$tempResult = [];
			$count1 = count($result);
			$count2 = count($next);
			$i = $j = 0;

			while ($i < $count1 && $j < $count2) {
				if ($result[$i] < $next[$j]) {
					$i++;
				} elseif ($result[$i] > $next[$j]) {
					$j++;
				} else {
					$tempResult[] = $result[$i];
					$i++;
					$j++;
				}
			}

			$result = $tempResult;
		}

		return $result;
	}


	/**
	 * 跳步求交集
	 */
	public function skip_intersection($arr1, $arr2)
	{

		// 系统交集函数
		if ($this->primarykeyType == 'UUID') {
			return array_intersect($arr1, $arr2);
		}

		sort($arr1);
		sort($arr2);
		$result = [];
		$count1 = count($arr1);
		$count2 = count($arr2);
		$i = $j = 0;
		$step = 1000;
		$count = 0;

		while (($i < $count1) && ($j < $count2)) {

			if ($arr1[$i] == $arr2[$j]) {

				$result[] = $arr1[$i];
				$i++;
				$j++;
				$count++;
			} else if ($arr1[$i] < $arr2[$j]) {
				if (($i + $step) < $count1) {
					if ($arr1[$i + $step] < $arr2[$j]) {
						$i = $i + $step;
						$count++;
					} else {
						$i++;
						$count++;
					}
				} else {
					$i++;
					$count++;
				}
			} else if ($arr1[$i] > $arr2[$j]) {
				if (($j + $step) < $count2) {
					if ($arr2[$j + $step] < $arr1[$i]) {
						$j = $j + $step;
						$count++;
					} else {
						$j++;
						$count++;
					}
				} else {
					$j++;
					$count++;
				}
			}
		}


		return $result;
	}


	/**
	 * 跳步求交集 支持多个数组
	 * 对int类型的多个数组求交集，效率比系统函数快太多
	 */
	public function multi_skip_intersection($array1 = [])
	{

		// 获取传入的所有参数
		$arrays = func_get_args();


		return $this->multi_intersection(...$arrays);

		// 系统交集函数
		if ($this->primarykeyType === 'UUID') {
			return array_intersect(...$arrays);
		} else {
			return array_intersect(...$arrays);
		}

		// 对每个int数组排序成递增形式
		$arrays = array_map(function ($p) {
			// 升序排列，且索引值从0开始
			sort($p);
			return $p;
		}, $arrays);

		// 踢出第一个数组，并保存，作为比对基础
		$result = array_shift($arrays);
		// 再依次拿出剩下的数组进行比对
		while (!empty($arrays)) {
			$next = array_shift($arrays);

			$tempResult = [];
			$count1 = count($result);
			$count2 = count($next);
			$i = $j = 0;
			$step = 1000;
			$count = 0;

			while (($i < $count1) && ($j < $count2)) {

				if ($result[$i] == $next[$j]) {

					$tempResult[] = $result[$i];
					$i++;
					$j++;
					$count++;
				} else if ($result[$i] < $next[$j]) {
					if (($i + $step) < $count1) {
						if ($result[$i + $step] < $next[$j]) {
							$i = $i + $step;
							$count++;
						} else {
							$i++;
							$count++;
						}
					} else {
						$i++;
						$count++;
					}
				} else if ($result[$i] > $next[$j]) {
					if (($j + $step) < $count2) {
						if ($next[$j + $step] < $result[$i]) {
							$j = $j + $step;
							$count++;
						} else {
							$j++;
							$count++;
						}
					} else {
						$j++;
						$count++;
					}
				}
			}

			$result = $tempResult;
		}

		return $result;
	}


	/**
	 * int 多个数组，取交集，强制升序排序
	 * 适合多个大数组
	 */
	public function multi_skip_intersection_bigdata($array1 = [])
	{

		// 获取传入的所有参数
		$arrays = func_get_args();

		return $this->multi_intersection(...$arrays);


		// 系统交集函数
		if ($this->primarykeyType === 'UUID') {
			return array_intersect(...$arrays);
		}

		// 对每个int数组排序成递增形式
		$arrays = array_map(function ($p) {
			// 升序排列，且索引值从0开始
			sort($p);
			return $p;
		}, $arrays);

		// 踢出第一个数组，并保存，作为比对基础
		$result = array_shift($arrays);
		// 再依次拿出剩下的数组进行比对
		while (!empty($arrays)) {
			$next = array_shift($arrays);

			$tempResult = [];
			$count1 = count($result);
			$count2 = count($next);
			$i = $j = 0;
			$step = 200;
			$count = 0;

			while (($i < $count1) && ($j < $count2)) {

				if ($result[$i] == $next[$j]) {

					$tempResult[] = $result[$i];
					$i++;
					$j++;
					$count++;
				} else if ($result[$i] < $next[$j]) {
					if (($i + $step) < $count1) {
						if ($result[$i + $step] < $next[$j]) {
							$i = $i + $step;
							$count++;
						} else {
							$i++;
							$count++;
						}
					} else {
						$i++;
						$count++;
					}
				} else if ($result[$i] > $next[$j]) {
					if (($j + $step) < $count2) {
						if ($next[$j + $step] < $result[$i]) {
							$j = $j + $step;
							$count++;
						} else {
							$j++;
							$count++;
						}
					} else {
						$j++;
						$count++;
					}
				}
			}

			$result = $tempResult;
		}

		return $result;
	}

	/**
	 * 方法重写 key相同，其值则合并为一个数组
	 * 无论key是字符串还是int，只要key相同，都会被合并
	 */
	public function array_merge_recursive($array1 = [])
	{
		$arrays = func_get_args();

		$result = array_shift($arrays);

		while (!empty($arrays)) {
			$next = array_shift($arrays);
			foreach ($next as $key => $value) {
				if (isset($result[$key])) {
					if (!is_array($result[$key]) && !is_array($value)) {
						$result[$key] = [$result[$key], $value];
					} else if (is_array($result[$key]) && !is_array($value)) {
						$result[$key][] = $value;
					} else if (!is_array($result[$key]) && is_array($value)) {
						$result[$key] = ($value[] = $result[$key]);
					} else if (is_array($result[$key]) && is_array($value)) {
						$result[$key] = $this->array_merge_recursive($result[$key], $value);
					}
				} else {
					$result[$key] = $value;
				}
			}
		}

		return $result;
	}


	/**
	 * 一维数组 按键值进行排序 倒序，值相同时，保持原顺序
	 */
	public function uasort_val($array)
	{
		// 自定义比较函数，当值相同时返回0，表示保持原顺序
		uasort($array, function ($a, $b) {
			if ($a === $b) {
				return 0;
			}
			return ($a < $b) ? 1 : -1;
		});

		return $array;
	}

	/**
	 * 二维数组 按照某个字段值排序，值相同时，保持原顺序
	 */
	public function usortRes($items, $field = '', $sort = 'desc')
	{


		// 创建一个关联数组来跟踪原始顺序
		$order = [];
		foreach ($items as $index => $item) {
			$order[$index] = $item[$field];
		}

		// 自定义比较函数
		function compareItems($a, $b, $order, $field, $sort)
		{
			// 如果id相同，则按照原始顺序排序
			if ($a[$field] == $b[$field]) {
				return array_search($a[$field], $order) <=> array_search($b[$field], $order);
			}
			if ($sort == 'desc') {
				// 否则按照id降序排序
				return $b[$field] <=> $a[$field];
			} else {
				// 升序
				return $a[$field] <=> $b[$field];
			}
		}

		// 使用usort进行排序
		usort($items, function ($a, $b) use ($order, $field, $sort) {
			return compareItems($a, $b, $order, $field, $sort);
		});

		return $items;
	}



	/**
	 * 判断字符串日期格式是否正确
	 */
	public function isValidDateString($date)
	{

		// 正则表达式匹配 YYYY-MM-DD 格式
		$pattern1 = '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/';
		// 正则表达式匹配 DD-MM-YYYY 格式
		$pattern2 = '/^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$/';

		// 正则表达式匹配 YYYY-MM-DD HH:MM:SS 格式
		$pattern3 = '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$/';
		// 正则表达式匹配 DD-MM-YYYY HH:MM:SS 格式
		$pattern4 = '/^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4} ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$/';

		// 检查日期字符串是否匹配任一模式
		if (preg_match($pattern1, $date) || preg_match($pattern2, $date) || preg_match($pattern3, $date) || preg_match($pattern4, $date)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 判断字符串日期格式是否只有年月日
	 */
	public function isNYRDateString($date)
	{

		// 正则表达式匹配 YYYY-MM-DD 格式
		$pattern1 = '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/';
		// 正则表达式匹配 DD-MM-YYYY 格式
		$pattern2 = '/^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$/';


		// 检查日期字符串是否匹配任一模式
		if (preg_match($pattern1, $date) || preg_match($pattern2, $date)) {
			return true;
		} else {
			return false;
		}
	}


	/**
	 * yield读取文件
	 */
	public function yield_fread_row()
	{
		return function ($dir) {
			$file = fopen($dir, "r");
			while ($line = fgets($file)) {
				yield $line;
			}
		};
	}


	/**
	 * 生成指定范围的hash值
	 */
	public function get_hash($key, $max)
	{
		$l = strlen($key);
		$h = 0x238f13af;
		while ($l--) {
			$h += ($h << 5);
			$h ^= ord(substr($key, $l, 1));
			$h &= 0x7fffffff;
		}

		return ($h % $max);
	}


	public function substr_before($str, $find, $is_contain = false)
	{

		if (!$is_contain) {
			$pos = stripos($str, $find);
			if ($pos !== false) {
				return substr($str, 0, $pos);
			} else {
				return $str;
			}
		}
		//包含 查找的内容
		else {
			$pos = stripos($str, $find);
			if ($pos !== false) {
				return substr($str, 0, stripos($str, $find) + strlen($find));
			} else {
				return $str;
			}
		}
	}


	private static function manualIntersection($array1, $array2)
	{
		$intersection = [];
		$hashMap = [];

		// 将第一个数组的元素存入哈希表
		foreach ($array1 as $value) {
			$hashMap[$value] = true;
		}
		// $hashMap = array_flip($array1);

		// 检查第二个数组中的元素是否在哈希表中
		foreach ($array2 as $value) {
			if (isset($hashMap[$value])) {
				$intersection[] = $value;
			}
		}
		unset($hashMap);
		return $intersection;
	}



	public function multi_intersection($array1 = [])
	{

		$arrays = func_get_args();
		$result = array_shift($arrays);
		while (!empty($arrays)) {
			$next = array_shift($arrays);
			$result = self::manualIntersection($result, $next);
		}

		return $result;
	}
}
