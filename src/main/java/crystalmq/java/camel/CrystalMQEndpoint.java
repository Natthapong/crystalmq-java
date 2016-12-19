package crystalmq.java.camel;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * Represents a CrystalMQ endpoint.
 */
public class CrystalMQEndpoint extends DefaultEndpoint {

    public CrystalMQEndpoint() {
    }

    public CrystalMQEndpoint(String uri, CrystalMQComponent component) {
        super(uri, component);
    }

    public CrystalMQEndpoint(String endpointUri) {
        super(endpointUri);
    }

    public Producer createProducer() throws Exception {
        return new CrystalMQProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new CrystalMQConsumer(this, processor);
    }

    public boolean isSingleton() {
        return true;
    }

    public boolean isLenientProperties() {
        return true;
    }
}
