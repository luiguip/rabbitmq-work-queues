package com.galvatron.producer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Hello world!
 *
 */
public class NewTask {
    private final static String QUEUE_NAME = "work_queue";
    
    public static void main( String[] args ) throws IOException, TimeoutException{
       
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.2");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = getMessage(args);
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        
        channel.close();
        connection.close();
    }
    
    private static String getMessage(String[] strings) {
        if(strings.length < 1) {
            return "Hello World!";
        }
        return joinStrings(strings, " ");
    }
    
    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for(int i = 1 ; i< length ; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
