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

//TODO : need to specific Exception later
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

        try {

                createClientSocket();

                MessagePack messagePack = new MessagePack();
                Template<Map<String, String>> template = tMap(TString, TString);

                while (true) {

                    try {

                        Map<String, String> map = messagePack.read(dataInputStream, template);

                        if (map.containsKey(MESSAGE)) {

                            String message = map.get(MESSAGE);
                            Exchange exchange = this.endpoint.createExchange();
                            exchange.getIn().setBody(message);
                            process.process(exchange);
                        }

                    } catch (Exception e) {

                        Thread.sleep(5000);
                        log.error("Consumer exception  : {}", e);
                        createClientSocket();
                    }
                }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    private void createClientSocket() throws IOException, InterruptedException {
           try {
               socket = new Socket(host, port);
               registerTopicAndChannel();
               dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
           } catch (Exception ex) {
               log.error("Can not connect to host : {}, port : {} ", host, port);
               log.error("Trying to connect again.");
               Thread.sleep(3000);
           }

    }

    private void registerTopicAndChannel() throws IOException {

        Map<String, Object> data = new HashMap();
        data.put(TOPIC, topic);
        data.put(CHANNEL, channel);

        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write(new MessagePack().write(data));
        dataOutputStream.flush();
    }

}
