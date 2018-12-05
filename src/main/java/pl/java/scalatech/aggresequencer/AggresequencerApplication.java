package pl.java.scalatech.aggresequencer;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import lombok.RequiredArgsConstructor;

@SpringBootApplication
public class AggresequencerApplication {
  private static final String DIRECT_START = "direct:start";
  private static final String SIZE = "size";
  private static final String ORDER = "order";
  private static final String CORRELATION_ID = "correlationId";

  @Autowired ProducerTemplate producerTemplate;

  public static void main(String[] args) {
    SpringApplication.run(AggresequencerApplication.class, args);
  }

  @Bean
  CommandLineRunner setup() {
    return args -> publicEvent();
  }

  void sendBodyAndHeaders(String body, Map<String, Object> headers) {
    producerTemplate.sendBodyAndHeaders(DIRECT_START, body, headers);
  }

  void publicEvent() {
    Map<String, Object> m3 = new HashMap<>();
    m3.put(CORRELATION_ID, "a");
    m3.put(SIZE, 3);
    m3.put(ORDER, 3);
    sendBodyAndHeaders("aa3", m3);

    Map<String, Object> m1 = new HashMap<>();
    m1.put(CORRELATION_ID, "a");
    m1.put(SIZE, 3);
    m1.put(ORDER, 1);
    sendBodyAndHeaders("aa1", m1);

    Map<String, Object> m5 = new HashMap<>();
    m5.put(CORRELATION_ID, "b");
    m5.put(SIZE, 3);
    m5.put(ORDER, 2);
    sendBodyAndHeaders("bb2", m5);

    Map<String, Object> m6 = new HashMap<>();
    m6.put(CORRELATION_ID, "b");
    m6.put(SIZE, 3);
    m6.put(ORDER, 3);
    sendBodyAndHeaders("bb3", m6);
    
    Map<String, Object> m2 = new HashMap<>();
    m2.put(CORRELATION_ID, "a");
    m2.put(SIZE, 3);
    m2.put(ORDER, 2);
    sendBodyAndHeaders("aa2", m2);

   

    Map<String, Object> m4 = new HashMap<>();
    m4.put(CORRELATION_ID, "b");
    m4.put(SIZE, 3);
    m4.put(ORDER, 1);
    sendBodyAndHeaders("bb1", m4);

    Map<String, Object> m7 = new HashMap<>();
    m7.put(CORRELATION_ID, "c");
    m7.put(SIZE, 1);
    m7.put(ORDER, 1);
    sendBodyAndHeaders("cc1", m7);
  }

  @Bean
  RouteBuilder routeBuilder() {
    return new SampleEventRoute();
  }

  @RequiredArgsConstructor
  class SampleEventRoute extends RouteBuilder {
    @Override
    public void configure() throws Exception {
      from(DIRECT_START)
          .process(exchange -> System.out.println("Input event : " + exchange.getIn().getBody()))
          .aggregate(header(CORRELATION_ID), new ResequencerStrategy())
          .completionSize(header(SIZE)) //here i can pass messages size in dynamic way
          .completionTimeout(2000)
          .log("${body}")
          .log("Completed by ${property.CamelAggregatedCompletedBy}")
          .split().body()
          .setBody(simple("${body.payload}"))
          .process(exchange -> System.out.println("Output event  : " + exchange.getIn().getBody()))
          .end();
    }
  }
}