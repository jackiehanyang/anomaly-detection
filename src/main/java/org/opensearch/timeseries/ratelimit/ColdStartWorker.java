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
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class ColdStartWorker<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, IndexableResultType>, CacheType extends TimeSeriesCache<RCFModelType>, IndexableResultType extends IndexableResult, IntermediateResultType extends IntermediateResult<IndexableResultType>, ModelManagerType extends ModelManager<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType>, SaveResultStrategyType extends SaveResultStrategy<IndexableResultType, IntermediateResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>>
    extends SingleRequestWorker<FeatureRequest> {
    private static final Logger LOG = LogManager.getLogger(ColdStartWorker.class);

    protected final ColdStarterType coldStarter;
    protected final CacheType cacheProvider;
    private final ModelManagerType modelManager;
    private final SaveResultStrategyType resultSaver;
    private final TaskManagerType taskManager;

    public ColdStartWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Setting<Integer> concurrency,
        Duration executionTtl,
        ColdStarterType coldStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        CacheType cacheProvider,
        AnalysisType context,
        ModelManagerType modelManager,
        SaveResultStrategyType resultSaver,
        TaskManagerType taskManager
    ) {
        super(
            workerName,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            concurrency,
            executionTtl,
            stateTtl,
            nodeStateManager,
            context
        );
        this.coldStarter = coldStarter;
        this.cacheProvider = cacheProvider;
        this.modelManager = modelManager;
        this.resultSaver = resultSaver;
        this.taskManager = taskManager;
    }

    @Override
    protected void executeRequest(FeatureRequest coldStartRequest, ActionListener<Void> listener) {
        String configId = coldStartRequest.getConfigId();

        String modelId = coldStartRequest.getModelId();
        if (null == modelId) {
            String error = String.format(Locale.ROOT, "Fail to get model id for request %s", coldStartRequest);
            LOG.warn(error);
            listener.onFailure(new RuntimeException(error));
            return;
        }
        ModelState<RCFModelType> modelState = createEmptyState(coldStartRequest, modelId, configId);

        ActionListener<List<IndexableResultType>> coldStartListener = ActionListener.wrap(r -> {
            // task id equals to null means it is real time and we want to cache
            nodeStateManager.getConfig(configId, context, coldStartRequest.getTaskId() == null, ActionListener.wrap(configOptional -> {
                try {
                    if (!configOptional.isPresent()) {
                        LOG
                            .error(
                                new ParameterizedMessage(
                                    "fail to load trained model [{}] to cache due to the config not being found.",
                                    modelState.getModelId()
                                )
                            );
                        return;
                    }
                    Config config = configOptional.get();

                    // score the current feature if training succeeded
                    if (modelState.getModel().isPresent()) {
                        String taskId = coldStartRequest.getTaskId();
                        if (r != null) {
                            for (int i = 0; i < r.size(); i++) {
                                IndexableResultType trainingResult = r.get(i);
                                resultSaver.saveResult(trainingResult, config);
                            }
                        }

                        long dataStartTime = coldStartRequest.getDataStartTimeMillis();
                        Sample currentSample = new Sample(
                            coldStartRequest.getCurrentFeature(),
                            Instant.ofEpochMilli(dataStartTime),
                            Instant.ofEpochMilli(dataStartTime + config.getIntervalInMilliseconds())
                        );
                        IntermediateResultType result = modelManager.getResult(currentSample, modelState, modelId, config, taskId);
                        resultSaver.saveResult(result, config, coldStartRequest, modelId);

                        // only load model to memory for real time analysis that has no task id
                        if (Strings.isEmpty(coldStartRequest.getTaskId())) {
                            boolean hosted = cacheProvider.hostIfPossible(configOptional.get(), modelState);
                            LOG
                                .debug(
                                    hosted
                                        ? new ParameterizedMessage("Loaded model {}.", modelState.getModelId())
                                        : new ParameterizedMessage("Failed to load model {}.", modelState.getModelId())
                                );
                        }
                    } else {
                        // run once scenario
                        String taskId = coldStartRequest.getTaskId();
                        if (taskId != null) {
                            Map<String, Object> updatedFields = new HashMap<>();
                            updatedFields.put(TimeSeriesTask.STATE_FIELD, TaskState.INACTIVE.name());
                            updatedFields.put(TimeSeriesTask.ERROR_FIELD, CommonMessages.NOT_ENOUGH_DATA);

                            taskManager.updateTask(taskId, updatedFields, ActionListener.wrap(updateResponse -> {
                                LOG.info("Updated task {} for config {}", taskId, configId);
                            }, e -> { LOG.error("Failed to update task: {} for config: {}", taskId, configId, e); }));
                        }
                    }
                } finally {
                    listener.onResponse(null);
                }
            }, listener::onFailure));

        }, e -> {
            try {
                if (ExceptionUtil.isOverloaded(e)) {
                    LOG.error("OpenSearch is overloaded");
                    setCoolDownStart();
                }
                nodeStateManager.setException(configId, e);
            } finally {
                listener.onFailure(e);
            }
        });

        coldStarter.trainModel(coldStartRequest, configId, modelState, coldStartListener);
    }

    protected abstract ModelState<RCFModelType> createEmptyState(FeatureRequest coldStartRequest, String modelId, String configId);
}
