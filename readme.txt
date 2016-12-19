CrystalMQ Camel Component Project
====================
This Project is a Camel component for CrystalMQ.



application.yml
==================
crystal:
        server : ip
        port :
          producer : 1234
          consumer : 1235
        topic : topic-name
        channel : channel-name


Router Example
===================

// set timer to send message every 10 secs to CrystalMQ
 from("timer://test?period=10s")
    .process(new Processor() {
        @Override
        public void process(Exchange exchange) throws Exception {

            String message = "Random message : " + UUID.randomUUID();
            System.out.println("Hi -> " + message);
            exchange.getIn().setBody(message);
        }
    })
    .to("crystalmq://producer?host={{crystal.server}}&port={{crystal.port.producer}}&topic={{crystal.topic}}");

//Consume from CrystalMQ
from("crystalmq://consumer?host={{crystal.server}}&port={{crystal.port.consumer}}&topic={{crystal.topic}}&channel={{crystal.channel}}")
        .process(new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                System.out.println("Consumer : " + exchange.getIn().getBody(String.class));
            }
        });


Java Consumer Code Example
=================================
import org.msgpack.MessagePack;
import org.msgpack.template.Template;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tMap;

public class Consumer {

    public static void main(String[] args) throws IOException {
        Socket socket = null;
        try {

            socket = new Socket("localhost", 1235);
            Map<String, Object> data = new HashMap();
            data.put("topic", "sunseries");
            data.put("channel", "ms-chiwa");

            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.write(new MessagePack().write(data));
            dataOutputStream.flush();
        } finally {

        }


        System.out.println("Client Started...");


        DataInputStream is = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        MessagePack messagePack = new MessagePack();
        Template<Map<String,String>> template = tMap(TString, TString);
        try {
            while (true) {

                Map<String,String> map = messagePack.read(is, template);

                System.out.println("====> " + map.get("message"));

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception: " + e.getMessage());
        }

    }

}


Java Producer Code Example
==========================
import com.google.gson.Gson;
import org.msgpack.MessagePack;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pea.chiwa on 12/13/16.
 */
public class Producer {

    public static void main(String[] args) throws IOException {

        try {

            Socket socket = new Socket("dockerlb.int.bkk1.sunseries.travel", 1234);


            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            EventNotification eventNotification = new Gson().fromJson(jsonString, EventNotification.class);
                Map<String, Object> data = new HashMap();
                data.put("topic", "sunseries");
                data.put("message", eventNotification.toJSONString());

                dataOutputStream.write(new MessagePack().write(data));
                dataOutputStream.flush();

            String str = "Message";

            data = new HashMap();
            data.put("topic", "sunseries");
            data.put("message", str);

            dataOutputStream.write(new MessagePack().write(data));
            dataOutputStream.flush();

        } finally {
           socket.close();
        }

    }
}
