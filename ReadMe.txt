Camel Component Project
====================
This Project is a template of a Camel component.

When you create a component project, you need to move the META-INF/services/org/apache/camel/component/${name}
file to META-INF/services/org/apache/camel/component/foo where "foo" is the URI scheme for your component and any
related endpoints created on the fly.

For more help see the Apache Camel documentation:

    http://camel.apache.org/writing-components.html


application.yml
==================
crystal:
        server : 172.16.1.250
        port :
          producer : 1234
          consumer : 1235
        topic : sunseries
        channel : ms-request-handler


Router Example
===================

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

        from("crystalmq://consumer?host={{crystal.server}}&port={{crystal.port.consumer}}&topic={{crystal.topic}}&channel={{crystal.channel}}")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Consumer : " + exchange.getIn().getBody(String.class));
                    }
                });