package pl.java.scalatech.aggresequencer;

import lombok.Value;

@Value
public class PayloadWrapper {

    private final String payload;
    private final int order;
    private final String correlationId;

}
