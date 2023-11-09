package com.ursolutions.dataflow.beam.pipeline;

import com.ursolutions.dataflow.beam.pipeline.step.ComposeSummaryStringFn;
import com.ursolutions.dataflow.beam.pipeline.step.ParseKeyValueFn;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.io.Serializable;

@Slf4j
public class LetterNumberPipelineFactory implements Serializable {

    private static final Long DURATION = 60L;

    public Pipeline build(LetterNumberPipelineOptions options) {
        log.info("Building pipeline");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, Integer>> letterNumberCollection = readToKeyValue(
                options.getInputSubscription(),
                pipeline);

        PCollection<KV<String, Integer>> summedCollection = applySumForLetter(letterNumberCollection);

        // side input aggregation transformations
        PCollection<Integer> aggregationCollectionOfValues = letterNumberCollection.apply(Values.create());
        PCollectionView<Long> countAll = aggregationCollectionOfValues
                .apply(Combine.globally(Count.<Integer>combineFn()).asSingletonView());

        PCollectionView<Integer> maxBetweenAll = aggregationCollectionOfValues
                .apply(Combine.globally(Max.ofIntegers()).asSingletonView());

        writeSummaryCollection(
                composeSummary(summedCollection, maxBetweenAll, countAll),
                options.getOutputTopic());

        return pipeline;
    }

    private PCollection<KV<String, Integer>> readToKeyValue(ValueProvider<String> subscription,
                                                            Pipeline pipeline) {
        return pipeline
                .apply("Fetch and convert the data",
                    PubsubIO.readStrings()
                            .withTimestampAttribute("timestamp_ms")
                            .fromSubscription(subscription))
                .apply("convert to key-value format", ParDo.of(new ParseKeyValueFn()))
                .apply("Perform windowing", Window.<KV<String, Integer>>into(
                        FixedWindows.of(Duration.standardSeconds(DURATION)))
                        .triggering(Repeatedly.forever(
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(DURATION))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes());

    }

    private PCollection<KV<String, Integer>> applySumForLetter(PCollection<KV<String, Integer>> letterNumberCollection) {
        return letterNumberCollection.apply("Count sum of the numbers per each key", Sum.integersPerKey());
    }

    private PCollection<String> composeSummary(PCollection<KV<String, Integer>> summedNumberCollection,
                                               PCollectionView<Integer> max,
                                               PCollectionView<@UnknownKeyFor @NonNull @Initialized Long> count) {
        return summedNumberCollection
                .apply("Perform windowing", Window.<KV<String, Integer>>into(
                        FixedWindows.of(Duration.standardSeconds(DURATION)))
                        .triggering(Repeatedly.forever(
                                AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(DURATION + 15))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("Generate summary for each row",
                    ParDo.of(new ComposeSummaryStringFn(max, count)).withSideInputs(max, count));
    }

    private void writeSummaryCollection(PCollection<String> composeSummary,
                                        ValueProvider<String> outputTopic) {
        composeSummary.apply("Publish", PubsubIO.writeStrings()
                .to(outputTopic));
    }
}
