package com.hello.reactor;

import reactor.core.publisher.Flux;

public class HelloReactor {

  public static void main(String[] args) {
    Flux<String> just = Flux.just("1", "2", "3");
    just.log()
        .map(
            a -> {
              System.out.println(a);
              return a;
            })
        .subscribe();
  }
}
