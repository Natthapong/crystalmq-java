package crystalmq.java.camel;

import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

/**
 * The CrystalMQ consumer.
 */
public class CrystalMQConsumer extends DefaultConsumer {

    private static final transient Logger log = LoggerFactory.getLogger(CrystalMQConsumer.class);

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TOPIC = "topic";
    public static final String CHANNEL = "channel";

    private final CrystalMQEndpoint endpoint;
    private Socket socket = null;

    public CrystalMQConsumer(CrystalMQEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {

        String host = this.getEndpoint().getEndpointConfiguration().getParameter(HOST);
        String port = this.getEndpoint().getEndpointConfiguration().getParameter(PORT);
        String topic = this.getEndpoint().getEndpointConfiguration().getParameter(TOPIC);
        String channel = this.getEndpoint().getEndpointConfiguration().getParameter(CHANNEL);

        try {

            Thread consumerThread = new Thread(
                    new ConsumerThread(this.endpoint,
                    this.getProcessor(),
                            host,
                            Integer.valueOf(port),
                            topic,
                            channel)
            );
            consumerThread.start();

        } catch (Exception ex) {

            log.error("Error on consumer router : {} ", ex.getMessage());
            Thread.sleep(3000);
            doStart();
        }

    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        log.info("Stopping CrystalMQ consumer");
        if (socket != null && socket.isConnected()) {
            socket.close();
        }
    }


}
