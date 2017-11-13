package com.hello.grpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

public final class HelloGrpc {

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
    ListenableFuture<PullResponse> pullResponse = cloudPubSubGRPCSubscriber.pull(request);

    Futures.addCallback(
        pullResponse,
        new FutureCallback<PullResponse>() {
          public void onSuccess(PullResponse pullResponse) {
            System.out.println("message received!");
            pullResponse
                .getReceivedMessagesList()
                .stream()
                .forEach(
                    receivedMessage ->
                        System.out.println(receivedMessage.getMessage().getData().toStringUtf8()));
          }

          public void onFailure(Throwable thrown) {
            System.out.println("error happened");
            System.out.println(thrown.getMessage());
          }
        });

    Thread.sleep(10000);
  }
}
