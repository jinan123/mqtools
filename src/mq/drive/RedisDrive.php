<?php

namespace mq\drive;

use BadFunctionCallException;
use Exception;
use Redis;

class RedisDrive{

    //队列操作对象
    protected $handler;
    //消费者
    protected $consumers;

    //连接配置
    protected $options = [
        'host'       => '127.0.0.1',
        'port'       => 6379,
        'password'   => '',
        'select'     => 0,
        'max'        => 8000,
        'consumers'  => null
    ];

    /**
     * 架构函数
     * @access public
     * @param  array $options 连接参数
     */
    public function __construct($options = []){
        if (!empty($options)) {
            $this->options = array_merge($this->options, $options);
        }

        if (extension_loaded('redis')) {
            $this->handler = new Redis;

            $this->handler->connect($this->options['host'], $this->options['port'], $this->options['timeout']);

            if ($this->options['password'] != '') {
                $this->handler->auth($this->options['password']);
            }

            if ( $this->options['select'] != 0) {
                $this->handler->select($this->options['select']);
            }
            //注册消费者
            $this->consumers = new $this->options['consumers'];
        } else {
            exit('not support: redis');
        }
    }

    /**
     * 将消息推入到队列
     * @param array $msgBody 消息体
     * @param string $queueName 队列名称
     * @throws Exception
     */
    public function push(array $msgBody,string $queueName="base-queue"){
        return $this->handler->lPush($queueName, json_encode($msgBody));
    }

    /**
     * 消费队列
     * @author jinanav 2020年1月17日15:11:44
     * @param string $queue 队列key名称
     * @return array
     */
    public function dequeue($queue = 'base-queue'){
        while (true){
            $data = $this->handler->rPop($queue);
            $data = json_decode($data,true)?:[];
            if(!empty($data)){
                //消费消息
                $this->consumers->deal($data);
            }else{
                sleep(1);
            }

        }
    }

    /**
     * 返回队列长度
     * @author jinanav 2021年1月26日17:25:52
     * @param string $queue 队列key名称
     * @return int
     */
    public function queueLen(string $queue = 'base-queue'){
        return $this->handler->lLen($queue);
    }



}