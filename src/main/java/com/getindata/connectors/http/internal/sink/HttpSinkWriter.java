package com.getindata.connectors.http.internal.sink;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

/**
 * Sink writer created by {@link com.getindata.connectors.http.HttpSink} to write to an HTTP
 * endpoint.
 *
 * <p>More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}
 * documentation.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {

    private static final String HTTP_SINK_WRITER_THREAD_POOL_SIZE = "4";

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    private final ScheduledExecutorService sinkWriterThreadPool;

    private final String endpointUrl;

    private final SinkHttpClient sinkHttpClient;

    private final Counter numRecordsSendErrorsCounter;

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 1000;

    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            SinkHttpClient sinkHttpClient,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates,
            Properties properties) {

        super(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests,
            maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes, bufferedRequestStates);
        this.endpointUrl = endpointUrl;
        this.sinkHttpClient = sinkHttpClient;

        var metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();

        int sinkWriterThreadPollSize = Integer.parseInt(properties.getProperty(
            HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE,
            HTTP_SINK_WRITER_THREAD_POOL_SIZE
        ));

        this.sinkWriterThreadPool =
            Executors.newScheduledThreadPool(
                sinkWriterThreadPollSize,
                new ExecutorThreadFactory(
                    "http-sink-writer-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));
    }

    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        submitWithRetries(requestEntries, requestResult, 0, INITIAL_BACKOFF_MS);
    }

    private void submitWithRetries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult,
            int attempt,
            long backoff) {

        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync((response, err) -> {
            if (err != null || !response.getFailedRequests().isEmpty()) {
                int failedRequestsNumber = err != null ? requestEntries.size()
                                                   : response.getFailedRequests().size();
                log.error("Http Sink failed to write {} requests", failedRequestsNumber);
                numRecordsSendErrorsCounter.inc(failedRequestsNumber);

                if (attempt < MAX_RETRIES) {
                    long nextBackoff = backoff * 2;
                    log.info("Retrying {} requests after {} ms (attempt {}/{})",
                             failedRequestsNumber, backoff, attempt + 1, MAX_RETRIES);

                    List<HttpSinkRequestEntry> failedRequestEntries = err != null
                            ? requestEntries
                            : convertToHttpSinkRequestEntries(response.getFailedRequests());

                    sinkWriterThreadPool.schedule(
                        () -> submitWithRetries(failedRequestEntries, requestResult,
                            attempt + 1, nextBackoff),
                        backoff, TimeUnit.MILLISECONDS);
                } else {
                    log.error("Http Sink failed to write requests after {} attempts", MAX_RETRIES);
                    requestResult.accept(err != null
                            ? requestEntries
                            : convertToHttpSinkRequestEntries(response.getFailedRequests()));
                }
            } else {
                requestResult.accept(Collections.emptyList());
            }
        }, sinkWriterThreadPool);
    }

    private List<HttpSinkRequestEntry> convertToHttpSinkRequestEntries(
            List<HttpRequest> httpRequests) {
        return httpRequests.stream()
                       .flatMap(request -> request.getElements().stream()
                               .map(element -> new HttpSinkRequestEntry(request.getMethod(),
                                    element)))
                       .collect(Collectors.toList());
    }

    @Override
    protected long getSizeInBytes(HttpSinkRequestEntry s) {
        return s.getSizeInBytes();
    }

    @Override
    public void close() {
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
