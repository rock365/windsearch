<?php

namespace WindSearch\Exceptions;

class WindException extends \Exception
{


    public function render($e)
    {
      
        echo '<div style="width:70%;text-align:left;font-size:18px;margin:70px auto 20px auto;word-break: break-all;">
        <p style="width:100%;height:40px;background-color:#eee;line-height:40px;color:#666;font-size:16px;text-indent:10px">WindSearch全文检索中间件异常报告</p>
        <p style="font-size:25px;text-indent:10px">' . $e->getMessage() . '</p>
        <p style="font-size:17px;text-indent:10px">位于 '.$e->getFile().' ['.$e->getLine().'行]'.'</p>
       </div>';
        // echo '<h1>['.$e->getCode().']'.$e->getMessage().'</h1>';
        // echo '位于 '.$e->getFile().' ['.$e->getLine().'行]';

    }
}
