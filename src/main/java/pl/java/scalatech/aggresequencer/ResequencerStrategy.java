package pl.java.scalatech.aggresequencer;

import static java.util.Comparator.comparingInt;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;

class ResequencerStrategy implements AggregationStrategy {
    private static final String ORDER = "order";
    private static final String CORRELATION_ID = "correlationId";

    @Override
    @SuppressWarnings("unchecked")
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            List<PayloadWrapper> al = new ArrayList<>();
            al.add(createAggregatorWrapper(newExchange));
            newExchange.getIn().setBody(al);
            return newExchange;
        }
        oldExchange.getIn().getBody(List.class).add(createAggregatorWrapper(newExchange));
        oldExchange.getIn().getBody(List.class).sort(comparingInt(PayloadWrapper::getOrder));
        return oldExchange;

    }

    PayloadWrapper createAggregatorWrapper(Exchange newExchange) {
        return new PayloadWrapper(
                newExchange.getIn().getBody(String.class),
                newExchange.getIn().getHeader(ORDER, Integer.class),
                newExchange.getIn().getHeader(CORRELATION_ID, String.class));
    }
}
