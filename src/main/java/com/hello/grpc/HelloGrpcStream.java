package com.hello.grpc;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.*;
import reactor.core.publisher.Flux;

public final class HelloGrpcStream {

  static {
    System.setProperty(
        "java.util.logging.config.file",
        "/Users/janos/Documents/workspaces/reactor_tests/src/main/resources/logging.properties");
  }

  public static void main(final String[] args) throws Exception {


    Flux<Object> flux =
        Flux.create(
            fluxSink -> {
              Subscriber subscriber =
                  Subscriber.defaultBuilder(
                          SubscriptionName.create("crm360-degrees", "janos-subscription"),
                          new MessageReceiver() {
                            @Override
                            public void receiveMessage(
                                PubsubMessage message, AckReplyConsumer consumer) {
                              System.out.println("received");
                              System.out.println(message.getData().toStringUtf8());
                              //                  consumer.nack();
                              fluxSink.next(message);
                            }
                          })
                      .build();

              fluxSink.onRequest(
                  value -> {
                    System.out.println("requested: " + value);
                  });

              subscriber.startAsync();
            });

    flux.subscribe(System.out::println);

    new Thread(
            () -> {
              while (true) ;
            })
        .start();
  }
}
