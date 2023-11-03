package com.ursolutions.dataflow.beam.pipeline.step;

import com.google.cloud.pubsublite.proto.PubSubMessage;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@Slf4j
@NoArgsConstructor
public class ParseKeyValueFn extends DoFn<String, KV<String, Integer>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext ctx) {
        String messageElement = ctx.element();

        String[] keyValueMessage = messageElement.split(",");
        ctx.output(KV.<String, Integer>of(keyValueMessage[0], Integer.parseInt(keyValueMessage[1])));
        log.info("Parsing to key-value -> {}-{}", keyValueMessage[0], keyValueMessage[1]);
    }
}
