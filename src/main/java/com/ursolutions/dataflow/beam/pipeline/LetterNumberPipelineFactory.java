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
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
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
    public Pipeline build(LetterNumberPipelineOptions options) {
        log.info("Building pipeline");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, Integer>> letterNumberCollection = convertToKeyValueWithWindowing(
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

    private PCollection<KV<String, Integer>> convertToKeyValueWithWindowing(ValueProvider<String> subscription,
                                                                            Pipeline pipeline) {
        return pipeline
                .apply("Fetch and convert the data",
                    PubsubIO.readStrings()
                            .withTimestampAttribute("timestamp_ms")
                            .fromSubscription(subscription))
                .apply("Perform windowing", Window.<String>into(
                        FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(AfterWatermark.pastEndOfWindow())
                        .withAllowedLateness(Duration.ZERO)
                        .accumulatingFiredPanes())
                .apply("convert to key-value format", ParDo.of(new ParseKeyValueFn()));
    }

    private PCollection<KV<String, Integer>> applySumForLetter(PCollection<KV<String, Integer>> letterNumberCollection) {
        return letterNumberCollection.apply("Count sum of the numbers per each key", Sum.integersPerKey());
    }

    private PCollection<String> composeSummary(PCollection<KV<String, Integer>> summedNumberCollection,
                                               PCollectionView<Integer> max,
                                               PCollectionView<@UnknownKeyFor @NonNull @Initialized Long> count) {
        return summedNumberCollection.apply("Generate summary for each row",
                ParDo.of(new ComposeSummaryStringFn(max, count)).withSideInputs(max, count));
    }

    private void writeSummaryCollection(PCollection<String> composeSummary,
                                        ValueProvider<String> outputTopic) {
        composeSummary.apply("Publish", PubsubIO.writeStrings()
                .to(outputTopic));
    }
}
