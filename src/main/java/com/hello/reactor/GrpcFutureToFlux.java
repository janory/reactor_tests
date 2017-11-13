package com.hello.reactor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.hello.grpc.CloudPubSubGRPCSubscriber;
import com.hello.grpc.ConnectorUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class GrpcFutureToFlux {

    static <T> CompletableFuture<T> buildCompletableFuture(
            final ListenableFuture<T> listenableFuture
    ) {
        //create an instance of CompletableFuture
        CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // propagate cancel to the listenable future
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };


        Futures.addCallback(
                listenableFuture,
                new FutureCallback<T>() {
                    public void onSuccess(T result) {
                        completable.complete(result);

                    }

                    public void onFailure(Throwable thrown) {
                        completable.completeExceptionally(thrown);

                    }
                });

        return completable;
    }


    public static void main(final String[] args) throws Exception {
        PullRequest request =
                PullRequest.newBuilder()
                        .setSubscription(
                                String.format(
                                        ConnectorUtils.CPS_SUBSCRIPTION_FORMAT,
                                        ConnectorUtils.CPS_PROJECT_CONFIG,
                                        "janos-subscription"))
                        .setReturnImmediately(false)
                        .setMaxMessages(1)
                        .build();

        final CloudPubSubGRPCSubscriber cloudPubSubGRPCSubscriber = new CloudPubSubGRPCSubscriber();

        Flux.interval(Duration.ofSeconds(5))
                .map(aLong -> cloudPubSubGRPCSubscriber.pull(request))
                .map(pullResponseListenableFuture -> Mono.fromFuture(buildCompletableFuture(pullResponseListenableFuture))
                        .flatMapMany(v ->
                                Flux.fromIterable(v.getReceivedMessagesList()).log()
                        )
                )
                .map(receivedMessageFlux -> receivedMessageFlux.log()
                        .subscribe(receivedMessage -> {
                                    ListenableFuture<Empty> listenableFuture = cloudPubSubGRPCSubscriber.ackMessages(AcknowledgeRequest.newBuilder().setSubscription(                String.format(
                                            ConnectorUtils.CPS_SUBSCRIPTION_FORMAT,
                                            ConnectorUtils.CPS_PROJECT_CONFIG,
                                            "janos-subscription")).addAckIds(receivedMessage.getAckId()).build());
                                    Futures.addCallback(
                                            listenableFuture,
                                            new FutureCallback<Empty>() {
                                                public void onSuccess(Empty empty) {
                                                    System.out.println("acked!");
                                                }

                                                public void onFailure(Throwable thrown) {
                                                    System.out.println("error happened");
                                                    System.out.println(thrown.getMessage());
                                                }
                                            });
                                }
                        ))
                .log().subscribe(System.out::println);


        new Thread(
                () -> {
                    while (true) ;
                })
                .start();
    }
}
