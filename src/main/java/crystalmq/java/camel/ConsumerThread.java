package crystalmq.java.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.msgpack.MessagePack;
import org.msgpack.template.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tMap;


public class ConsumerThread implements Runnable {

    private static final transient Logger log = LoggerFactory.getLogger(ConsumerThread.class);

    public static final String TOPIC = "topic";
    public static final String CHANNEL = "channel";

    private Socket socket = null;
    private Endpoint endpoint;
    private Processor process;

    private String host;
    private int port;
    private String topic;
    private String channel;
    private DataInputStream dataInputStream;

    public static final String MESSAGE = "message";

    /**
     * @param endpoint
     * @param process
     * @param host
     * @param port
     * @param topic
     * @param channel
     * @throws IOException
     */
    public ConsumerThread(Endpoint endpoint,
                          Processor process,
                          String host,
                          int port,
                          String topic,
                          String channel) throws IOException {

        this.endpoint = endpoint;
        this.process = process;
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.channel = channel;


    }

    @Override
    public void run() {

    // while (true) {
         try {

             consumeMessage();

         } catch (Exception ex) {
             log.error("================================");
             log.error("Consumer error {} ", ex.getMessage());
             log.error("================================");
             ex.printStackTrace();
             /*log.error("Consume message got and exception : {}, trying to create a new connection.", ex.getMessage());
             try {
                 Thread.sleep(3000);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }*/
             throw new RuntimeException(ex);
         }
     //}

    }

    private void consumeMessage() throws Exception {

            createClientSocket();

            MessagePack messagePack = new MessagePack();
            Template<Map<String, String>> template = tMap(TString, TString);

            while (true) {

                Map<String, String> map = messagePack.read(dataInputStream, template);
                if (map.containsKey(MESSAGE)) {

                    String message = map.get(MESSAGE);
                    try {

                        Exchange exchange = this.endpoint.createExchange();
                        exchange.getIn().setBody(message);
                        process.process(exchange);
                    } catch (Exception ex) {

                        log.error("Can not send Camel Exchange to next endpoint with message : {} ", message);
                    }
                }
            }

    }

    /**
     *
     * @throws Exception when have and IOException
     */
    private void createClientSocket() throws Exception {

            socket = new Socket(host, port);
            Map<String, Object> data = new HashMap();
            data.put(TOPIC, topic);
            data.put(CHANNEL, channel);

            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.write(new MessagePack().write(data));
            dataOutputStream.flush();
            dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    }


}
