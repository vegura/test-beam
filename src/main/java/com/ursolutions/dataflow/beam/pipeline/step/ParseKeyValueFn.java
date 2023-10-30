package com.ursolutions.dataflow.beam.pipeline.step;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
@NoArgsConstructor
public class ParseKeyValueFn extends DoFn<PubsubMessage, KV<String, Integer>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext ctx) {
        PubsubMessage messageElement = ctx.element();
//        if (Objects.nonNull(messageElement) && !Objects.nonNull(messageElement.getPayload())) return;

        String[] keyValueMessage = new String(messageElement.getPayload(), StandardCharsets.UTF_8).split(",");
        ctx.output(KV.<String, Integer>of(keyValueMessage[0], Integer.parseInt(keyValueMessage[1])));
    }
}
