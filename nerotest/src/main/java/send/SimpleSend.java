package send;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.Map;


public class SimpleSend {
    private static final String producerGroup = "GROUP_TEST";
    private static final String testTopic = "TOPIC_TEST";







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
        defaultMQProducer.setNamesrvAddr("39.105.81.92:9876");
        defaultMQProducer.setSendMsgTimeout(100000);
        defaultMQProducer.start();


        Message message = new Message();
        message.setTopic(testTopic);

        Map<String,String> object = new HashMap<>();
        object.put("username","nero");
        object.put("password","123456");
        message.setBody(JSON.toJSONString(object).getBytes());


        SendResult sendResult = defaultMQProducer.send(message);
        System.out.println(sendResult);
        defaultMQProducer.shutdown();

    }








}
