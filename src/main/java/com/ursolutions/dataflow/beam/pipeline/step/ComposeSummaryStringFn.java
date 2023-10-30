package com.ursolutions.dataflow.beam.pipeline.step;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.time.Instant;

@Slf4j
public class ComposeSummaryStringFn extends DoFn<KV<String, Integer>, String> {
    private final PCollectionView<@UnknownKeyFor @NonNull @Initialized Integer> maxValueView;
    private final PCollectionView<@UnknownKeyFor @NonNull @Initialized Long> quantityView;

    public ComposeSummaryStringFn(PCollectionView<@UnknownKeyFor @NonNull @Initialized Integer> maxValueView,
                                  PCollectionView<@UnknownKeyFor @NonNull @Initialized Long> quantityView) {
        this.maxValueView = maxValueView;
        this.quantityView = quantityView;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        StringBuilder resultOut = new StringBuilder();
        KV<String, Integer> element = context.element();

        Integer maxValue = context.sideInput(maxValueView);
        Long quantity = context.sideInput(quantityView);

        Long timeStampNow = Instant.now().getEpochSecond() / 60L;
        resultOut.append(timeStampNow).append(",");

        resultOut.append(element.getKey()).append(",");

        resultOut.append(element.getValue()).append(",");
        resultOut.append(maxValue).append(",");
        resultOut.append(quantity);

        log.info("the result is -> {}", resultOut);
        context.output(resultOut.toString());
    }
}
