package crystalmq.java.camel;

import com.google.gson.Gson;
import crystalmq.java.camel.Exception.ConsumerException;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.Socket;

/**
 * Created by pea.chiwa on 12/14/16.
 */
public class ConsumerThread implements Runnable {

    private Socket socket;
    private Endpoint endpoint;
    private Processor process;
    public static final String MESSAGE = "message";

    public ConsumerThread(Socket socket, Endpoint endpoint, Processor process) {

        this.socket = socket;
        this.endpoint = endpoint;
        this.process = process;
    }

    @Override
    public void run() {
        try {

            DataInputStream is = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            while (true) {

                byte[] byteData = receive(is);
                Value value = new MessagePack().read(byteData);

                Exchange exchange = this.endpoint.createExchange();

                if (value.isMapValue()) {

                    MapValue mapValue = value.asMapValue();
                    String message = mapValue.toString();
                    CqmMessage cqmMessage = new Gson().fromJson(message, CqmMessage.class);
                    exchange.getIn().setBody(cqmMessage.getMessage());
                } else {

                    exchange.getIn().setBody(null);
                }
                process.process(exchange);
            }

        } catch (Exception e) {
            throw new ConsumerException(e.getMessage());
        }
    }

    public static byte[] receive(DataInputStream is) throws Exception {
        try {
            byte[] inputData = new byte[1024];
            is.read(inputData);
            return inputData;
        } catch (Exception exception) {
            throw exception;
        }
    }
}
