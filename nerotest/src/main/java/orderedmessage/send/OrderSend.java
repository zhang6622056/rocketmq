package orderedmessage.send;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderSend {



    private static final String producerGroup = "GROUP_ORDER_TEST";
    private static final String testTopic = "TOPIC_ORDER_TEST";
    private static final String nameServer = "39.105.81.92:9876";






    /***
     *
     * 测试发送消息
     * @author Nero
     * @date 2019-09-17
     * *@param: args
     * @return void
     */
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        defaultMQProducer.setNamesrvAddr(nameServer);
        defaultMQProducer.setSendMsgTimeout(100000);
        defaultMQProducer.start();

        Message message = new Message();
        message.setTopic(testTopic);

        for (int i = 0 ; i < 100 ; i++){
            int orderId = i;

            message.setKeys(String.valueOf(orderId));
            Map<String,String> object = new HashMap<>();
            object.put("username","nero"+i);
            object.put("password","123456");
            message.setBody(JSON.toJSONString(object).getBytes());
            SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer orderId = (Integer) arg;
                    int queueIndex = orderId % mqs.size();
                    return mqs.get(queueIndex);
                }
            },orderId);

            System.out.println(sendResult);
        }

        defaultMQProducer.shutdown();

    }




}
