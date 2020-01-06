package orderedmessage.consumer;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/***
 *
 * 顺序消费
 * @author Nero
 * @date 2019-09-18
 * *@param: null
 * @return
 */
public class OrderConsume {


    private static final String producerGroup = "GROUP_ORDER_TEST";
    private static final String testTopic = "TOPIC_ORDER_TEST";
    private static final String nameServer = "39.105.81.92:9876";



    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup(producerGroup);
        consumer.setMessageModel(MessageModel.CLUSTERING);

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return null;
            }
        });




        consumer.start();




    }




}
