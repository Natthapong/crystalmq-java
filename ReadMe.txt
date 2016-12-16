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


