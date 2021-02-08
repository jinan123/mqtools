<?php


namespace mq;



/**
 * Class MqService
 * @package think
 * @method sendMessage(array $msgBody,string $queueName="base-queue") static 推送消息到队列
 * @method dequeue(string $queueName="base-queue",$autoAck = true) static 消费队列消息
 * @method queueLen(string $queue = 'base-queue') 返回队列未校服数据数量
 */
class MqService {
    //配置文件
    protected static $config = [];
    //消息队列组
    protected static $mqService;
    //消息队列名称

    /**
     * 切换数据库连接
     * @access public
     * @param  array $config 连接配置
     * @return MqService
     */
    public static function connect(array $config = [],$name = false){
        //self::$config = config('mq.');
        //创建mq操作实例
        self::$config = array_merge(self::$config,$config);
        $mqServiceName = "mq\\drive\\".self::$config['type']."Drive";
        $name  ?: md5(serialize(self::$config));
        if(isset(self::$mqService[$name])){
            return self::$mqService[$name];
        }
        // 创建MQ对象实例
        return self::$mqService[$name] = new $mqServiceName(self::$config);
    }

    /**
     * 调用函数
     * @param $method
     * @param $args
     * @return mixed
     */
    public static function __callStatic($method, $args){
        return call_user_func_array([static::connect(), $method], $args);
    }
}