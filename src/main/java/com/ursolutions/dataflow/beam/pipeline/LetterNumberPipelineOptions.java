package com.ursolutions.dataflow.beam.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface LetterNumberPipelineOptions extends DataflowPipelineOptions {

    @Description("The cloud pub/sub subscription")
    @Default.String("projects/freedom-ukraine/subscriptions/letter-number-input-sub")
    ValueProvider<String> getInputSubscription();
    void setInputSubscription(ValueProvider<String> subscription);

    @Description("The output pub/sub topic where the result would be placed")
    @Default.String("projects/freedom-ukraine/topics/letter-number-summary-out")
    ValueProvider<String> getOutputTopic();
    void setOutputTopic(ValueProvider<String> outputTopic);

    @Description("Topic where the result with exception will be placed")
    @Default.String("projects/freedom-ukraine/topics/letter-number-error-out")
    String getErrorResultTopic();
    void setErrorResultTopic(String errorResultTopic);

    @Description("Windowing delay in minutes for allowed lateness.")
    @Default.Long(5)
    Long getAllowedWindowDelayInMins();
    void setAllowedWindowDelayInMins(Long allowedWindowDelayInMins);

    @Description("Window time in seconds. ")
    @Default.Long(10)
    ValueProvider<Long> getWindowTimeInSeconds();
    void setWindowTimeInSeconds(ValueProvider<Long> windowTimeInSeconds);

    @Description("Max No. of elements in window")
    @Default.Integer(1000)
    Integer getWindowElementsSizeLimit();
    void setWindowElementsSizeLimit(Integer windowElementsSizeLimit);
}
