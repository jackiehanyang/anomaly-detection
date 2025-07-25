/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;

/**
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 * @param <BatchRequestType> Batch request type like BulkRequest
 * @param <BatchResponseType> Response type like BulkResponse
 */
public abstract class BatchWorker<RequestType extends QueuedRequest, BatchRequestType, BatchResponseType> extends
    ConcurrentWorker<RequestType> {
    private static final Logger LOG = LogManager.getLogger(BatchWorker.class);
    protected int batchSize;

    public BatchWorker(
        String queueName,
        long heapSize,
        int singleRequestSize,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService circuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Setting<Integer> batchSizeSetting,
        Duration stateTtl,
        NodeStateManager timeSeriesNodeStateManager,
        AnalysisType context
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            concurrencySetting,
            executionTtl,
            stateTtl,
            timeSeriesNodeStateManager,
            context
        );
        this.batchSize = batchSizeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(batchSizeSetting, it -> batchSize = it);
    }

    /**
     * Used by subclasses to creates customized logic to send batch requests.
     * After everything finishes, the method should call listener.
     * @param request Batch request to execute
     * @param listener customized listener
     */
    protected abstract void executeBatchRequest(BatchRequestType request, ActionListener<BatchResponseType> listener);

    /**
     * We convert from queued requests understood by AD to batchRequest understood by OpenSearch.
     * @param toProcess Queued requests
     * @return batch requests
     */
    protected abstract BatchRequestType toBatchRequest(List<RequestType> toProcess);

    @Override
    protected void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback) {

        List<RequestType> toProcess = getRequests(batchSize);

        // it is possible other concurrent threads have drained the queue
        if (false == toProcess.isEmpty()) {
            final List<String> inflights = new ArrayList<>();
            for (RequestType request : toProcess) {
                String configId = request.getConfigId();
                if (configId != null) {
                    inflightConfigs.add(configId);
                    inflights.add(configId);
                }
            }

            BatchRequestType batchRequest = toBatchRequest(toProcess);

            ThreadedActionListener<BatchResponseType> listener = new ThreadedActionListener<>(
                LOG,
                threadPool,
                threadPoolName,
                getResponseListener(toProcess, batchRequest),
                false
            );

            final ActionListener<BatchResponseType> listenerWithRelease = ActionListener.runAfter(listener, afterProcessCallback);
            executeBatchRequest(batchRequest, ActionListener.runAfter(listenerWithRelease, () -> {
                if (!inflights.isEmpty()) {
                    inflightConfigs.removeAll(inflights);
                }
            }));
        } else {
            emptyQueueCallback.run();
        }
    }

    /**
     * Used by subclasses to creates customized logic to handle batch responses
     * or errors.
     * @param toProcess Queued request used to retrieve information of retrying requests
     * @param batchRequest Batch request corresponding to toProcess. We convert
     *  from toProcess understood by AD to batchRequest understood by ES.
     * @return Listener to BatchResponse
     */
    protected abstract ActionListener<BatchResponseType> getResponseListener(List<RequestType> toProcess, BatchRequestType batchRequest);
}
