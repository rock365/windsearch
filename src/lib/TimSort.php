<?php
namespace WindSearch\Core;

class Timsort {
    
    private $MIN_MERGE = 32;

    public function sort(&$array) {

        $n = count($array);
        if ($n < 2) {
            return;
        }

        // Divide the array into chunks and sort each chunk using insertion sort
        for ($i = 0; $i < $n; $i += $this->MIN_MERGE) {
            $this->insertionSort($array, $i, min($i + $this->MIN_MERGE - 1, $n - 1));
        }

        // Merge the sorted chunks
        for ($size = $this->MIN_MERGE; $size < $n; $size = 2 * $size) {
            for ($left = 0; $left < $n; $left += 2 * $size) {
                $mid = min($n - 1, $left + $size - 1);
                $right = min($n - 1, $left + 2 * $size - 1);
                if ($mid < $right) {
                    $this->merge($array, $left, $mid, $right);
                }
            }
        }
    }

    private function insertionSort(&$array, $left, $right) {
        for ($i = $left + 1; $i <= $right; $i++) {
            $temp = $array[$i];
            $j = $i - 1;
            while ($j >= $left && $array[$j] > $temp) {
                $array[$j + 1] = $array[$j];
                $j--;
            }
            $array[$j + 1] = $temp;
        }
    }

    private function merge(&$array, $left, $mid, $right) {
        $temp = [];
        $i = $left;
        $j = $mid + 1;
        $k = 0;

        while ($i <= $mid && $j <= $right) {
            if ($array[$i] <= $array[$j]) {
                $temp[$k++] = $array[$i++];
            } else {
                $temp[$k++] = $array[$j++];
            }
        }

        while ($i <= $mid) {
            $temp[$k++] = $array[$i++];
        }

        while ($j <= $right) {
            $temp[$k++] = $array[$j++];
        }

        for ($p = 0; $p < $k; $p++) {
            $array[$left + $p] = $temp[$p];
        }
    }
}


