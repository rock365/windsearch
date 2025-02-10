<?php

namespace WindSearch\Core;

class Boot
{
    public function init()
    {
        error_reporting(0);
        // 错误处理程序 notice warning error
        // set_error_handler([$this, 'appError']);
        // 异常处理程序
        set_exception_handler([$this, 'exceptions']);
    }

    /**
     * 异常处理程序
     * @param $e 系统异常类对象 或自定义的异常类对象
     */
    public function exceptions($e)
    {
       
        // 被自定义的异常类捕获
        if (method_exists($e, 'render')) {
            // 调用自定义类里面的render方法，显示异常信息
            $e->render($e);
        }
        // 未被自定义的异常类捕获
        else {
            // die($e);
            echo '<div style="width:70%;text-align:left;font-size:18px;margin:70px auto 20px auto;word-break: break-all;">
            <p style="width:100%;height:40px;background-color:#eee;line-height:40px;color:#666;font-size:16px;text-indent:10px">WindSearch全文检索中间件异常报告</p>
            <p style="font-size:25px;text-indent:10px;">' . $e->getMessage() . '</p>
            <p style="font-size:17px;text-indent:10px">位于 '.$e->getFile().' ['.$e->getLine().'行]'.'</p>
        </div>';
            exit;
        }
    }

    /**
     * 错误处理程序
     * 例如运行时错误:notice warning error
     */
    public static function appError($errno, $errstr, $errfile = '', $errline = 0)
    {
        $mapping = [
            1 => ['E_ERROR', '致命错误'],
            2 => ['E_WARNING', '警告'],
            3 => ['E_NOTICE', '通知'],
            4 => ['E_PARSE', '编译解析错误'],
            5 => ['E_CORE_ERROR', '启动时内核初始化错误'],
            6 => ['E_CORE_WARNING', '启动时内核初始化非致命错误'],
            7 => ['E_COMPILE_ERROR', '编译致命错误'],
            8 => ['E_COMPILE_WARNING', '编译警告'],
            9 => ['E_USER_ERROR', '致命错误'],
            10 => ['E_USER_WARNING', '警告'],
            11 => ['E_USER_NOTICE', '通知'],
            12 => ['E_STRICT', '编码标准化警告'],
            13 => ['E_RECOVERABLE_ERROR', '接受到可恢复的致命错误'],
            12 => ['E_STRICT', '编码标准化警告'],
        ];
        $error = $mapping[$errno][0] . '[' . $mapping[$errno][1] . ']:  <b>' . $errstr . '</b>';
        $error2 = " $errfile on line $errline";
        echo '<div style="width:70%;text-align:left;font-size:18px;margin:70px auto 20px auto;">
        <p style="width:100%;height:40px;background-color:#eee;line-height:40px;color:#666;font-size:16px;text-indent:10px">PHP全文检索中间件错误报告</p>
        <p style="font-size:25px;text-indent:10px;">' . $error . '</p><p style="font-size:18px;text-indent:10px;">' . $error2 . '</p>
       </div>';
        // exit;
    }
}
