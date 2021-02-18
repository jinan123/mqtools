<?php

namespace mq\drive;

use Exception;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * rabbitMq操作类
 * Class RabbitMqDrive
 * @package mq\drive
 */
class RabbitMqDrive{

    //rabbitMq连接对象
    protected $handler;
    //队列操作对象
    protected $channel;

    //交换机类型
    protected $consumers;
    //是否自动ack应答
    protected $autoAck = true;

    //连接配置
    protected $options = [
        'host'          => '127.0.0.1',
        'port'          => 5672,
        'password'      => '',
        'user'          => '',
        'vhost'         => '/',
        'debug'         => false,
        'exchangeType'  => 'direct',
        'exchangeName'  => 'base-exchange',
        'routingKey'    => 'base-routing',
        'queue'         => 'base-queue',
        'consumers'     => null
    ];

    /**
     * 初始化
     * RabbitMqDrive constructor.
     * @param array $options 配置参数
     */
    public function __construct(array $options = []){
        !empty($options) && $this->options = array_merge($this->options,$options);
        try{
            $this->handler =  new AMQPStreamConnection(
                $this->options['host'],
                $this->options['port'],
                $this->options['user'],
                $this->options['password'],
                $this->options['vhost']
            );
            //注册消费者
            if(!empty($this->options['consumers'])){
                $this->consumers = new $this->options['consumers'];
            }
            $this->channel = $this->handler->channel();
            $this->createExchange();
        }catch (Exception $e){
            if($this->options['debug']){
                exit('exception: '.$e->getMessage());
            }
            exit(" Server exception");
        }

    }

    /**
     * 将消息推入到队列
     * @author jinanav 2021年1月26日11:46:23
     * @return mixed
     */
    private function createExchange(){
        //声明初始化交换机
        $this->channel->exchange_declare($this->options['exchangeName'], $this->options['exchangeType'], false, true, false);
        //声明初始化一条队列
        $this->channel->queue_declare($this->options['queue'], false, true, false, false);
    }


    /**
     * 将消息推入到队列
     * @author jinanav 2021年1月26日11:46:23
     * @param array $msgBody 消息体
     * @param string $queueName 队列名称
     * @throws Exception
     */
    public function sendMessage(array $msgBody,string $queueName="base-queue"){
        //将队列与某个交换机进行绑定，并使用路由关键字
        $this->channel->queue_bind($queueName, $this->options['exchangeName'], $this->options['routingKey']);
        //生成消息并写入到队中
        $msg = new AMQPMessage(json_encode($msgBody,JSON_UNESCAPED_UNICODE), ['content_type' => 'text/plain', 'delivery_mode' => 2]); //生成消息
        //推送消息到某个交换机
        $this->channel->basic_publish($msg,$this->options['exchangeName'], $this->options['routingKey']);
    }

    /**
     * 读取队列消息
     * @author jinanav 2021年1月26日11:46:23
     * @param string $queueName 队列名称
     * @param string $exchangeName 交换机
     * @param string $routingKey 路由key
     * @throws Exception
     */
    public function dequeue(string $queueName="base-queue",$autoAck = true){
        $this->autoAck = $autoAck;
        //将队列与某个交换机进行绑定，并使用路由关键字 ,消费者与生产者都启用 以免队列不存在报异常
        $this->channel->queue_bind($queueName, $this->options['exchangeName'], $this->options['routingKey']);
        $this->channel->basic_consume($queueName, '', false, $this->autoAck, false, false, function($msg){$this->dealMsg($msg);});
        //监听消息
        while(count($this->channel->callbacks)){
            $this->channel->wait();
        }
    }

    /**
     * 消费消息
     * @author jinanav 2021年1月26日11:46:23
     * @param object $msg 消息体
     * @return mixed
     */
    public function dealMsg($msg){
        $info = json_decode($msg->body,true);
        if(empty($this->consumers)) exit("There are no consumers available");
        $this->consumers->deal($info);
        //开始处理
        if(!$this->autoAck) {
            //手动ack应答
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        }
    }

    /**
     * 读取队列中的数量
     * @author jinanav 2021年1月26日11:46:49
     * @return mixed
     */
    public function len(){
        return count([]);
    }

    /**
     * 关闭连接以及队列
     */
    public function close(){
        $this->channel->close();
        $this->handler->close();
    }

    /**
     * 析构函数
     */
    public function __destruct(){
        $this->close();
    }


}