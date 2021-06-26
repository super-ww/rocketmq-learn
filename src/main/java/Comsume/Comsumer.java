package Comsume;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Comsumer {
    public static void main(String[] args) throws MQClientException {
        //连接redis
        Jedis jedis = new Jedis("124.71.189.104", 6379);
        jedis.connect();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consume_group1");
        //设置nameserver
        consumer.setNamesrvAddr("82.156.17.108:9876;;124.71.189.104:9876");
        //订阅主题，即设定要拉取哪个主题的消息，第二个参数*代表拉取该主题下的所有TAG
        consumer.subscribe("TopicTest", "*");
        //设置名字
        consumer.setInstanceName("s1");

        //设置线程数
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(3);

        //lua脚本
        String script = "\nif redis.call('EXISTS',KEYS[1]) == 1 then \n" +
                "return 0 \n" +
                "else \n" +
                "redis.call('SET',KEYS[1],1) \n" +
                "return 1 \n" +
                "end \n";
        //加载lua脚本，调用eval函数的话就不用这一步
        //String shal = jedis.scriptLoad(script);

        //注册监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for (MessageExt msg : list) {
    //                    System.out.println(msg);
                        String key = msg.getKeys();

                        //执行lua脚本来判断消息是否已经消费了
                        Long result=(Long)jedis.eval(script,1,key);
                        //System.out.println(result);

                        //已经消费，直接跳过
                        if(result==0) {
                            System.out.println("key: "+key+" 已存在，无需尝试...");
                            continue;
                        }

                        //还未消费，进行消费
                        String msgId = msg.getMsgId();
                        System.out.println("thread: " + Thread.currentThread().getName() + "    key: " + key + "    msgId: " + msgId + "    " + new String(msg.getBody()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                System.out.println(list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("consume start");
    }
}
