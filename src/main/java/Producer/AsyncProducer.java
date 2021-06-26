package Producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer=new DefaultMQProducer("Asyncproducer_group1");
        producer.setNamesrvAddr("82.156.17.108:9876;124.71.189.104:9876");
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int massageCount=100;
        final CountDownLatch countDownLatch=new CountDownLatch(massageCount);
        for(int i=0;i<massageCount;i++) {
            try {
                final int index=i;
                Message msg=new Message("Async_topic_1",
                        "Tag1",
                        "OrderId1",
                        "helloWorld".getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();

    }
}
