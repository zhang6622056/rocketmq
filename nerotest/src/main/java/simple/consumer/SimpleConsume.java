package simple.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class SimpleConsume {


    private static final String consumeGroup = "GROUP_TEST";
    private static final String testTopic = "TOPIC_TEST";
    private static final String nameServer = "39.105.81.92:9876";





    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup(consumeGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.subscribe(testTopic,"*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                        for (MessageExt msg : msgs){
                            System.out.println(msg);
                        }
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
        );
        consumer.start();








    }



}
