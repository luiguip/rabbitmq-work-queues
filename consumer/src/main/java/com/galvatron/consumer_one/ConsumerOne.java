package com.galvatron.consumer_one;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ConsumerOne {
    private final static String QUEUE_NAME = "work_queue";
    
    public static void main( String[] args ) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.2");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                throws IOException {
              String message = new String(body, "UTF-8");
              System.out.println(" [x] Received '" + message + "'");
              try {
                  doWork(message);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                  System.out.println(" [x] Done");
                }
              }
            };
            boolean autoAck = true; // acknowledgment is covered below
            channel.basicConsume(QUEUE_NAME, autoAck, consumer);
        
    }
    
    private static void doWork(String task) throws InterruptedException {
        for(char ch: task.toCharArray()) {
            if(ch == '.') Thread.sleep(1000);
        }
    }
}
