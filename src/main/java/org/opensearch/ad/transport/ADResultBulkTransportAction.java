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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_INDEX_PRESSURE_HARD_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.client.Client;
import org.opensearch.client.RequestOptions;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.ResultBulkTransportAction;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public class ADResultBulkTransportAction extends ResultBulkTransportAction<AnomalyResult, ADResultWriteRequest, ADResultBulkRequest> {

    private static final Logger LOG = LogManager.getLogger(ADResultBulkTransportAction.class);
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public ADResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(
            ADResultBulkAction.NAME,
            transportService,
            actionFilters,
            indexingPressure,
            settings,
            client,
            AD_INDEX_PRESSURE_SOFT_LIMIT.get(settings),
            AD_INDEX_PRESSURE_HARD_LIMIT.get(settings),
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ADResultBulkRequest::new
        );
        this.clusterService = clusterService;
        this.client = client;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
    }

    @Override
    protected BulkRequest prepareBulkRequest(float indexingPressurePercent, ADResultBulkRequest request) {
        BulkRequest bulkRequest = new BulkRequest();
        List<ADResultWriteRequest> results = request.getResults();

        for (ADResultWriteRequest resultWriteRequest : results) {
            AnomalyResult result = resultWriteRequest.getResult();
            String resultIndex = resultWriteRequest.getResultIndex();

            // Add result based on indexing pressure
            if (shouldAddResult(indexingPressurePercent, result)) {
                addResult(bulkRequest, result, resultIndex);
                addToFlattenedIndexIfExists(bulkRequest, result, resultIndex);
            }
        }

        return bulkRequest;
    }

    /**
     * Determines whether a result should be added based on indexing pressure and result priority.
     */
    private boolean shouldAddResult(float indexingPressurePercent, AnomalyResult result) {
        if (indexingPressurePercent <= softLimit) {
            // Always add when below soft limit
            return true;
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            return result.isHighPriority() || random.nextFloat() < acceptProbability;
        } else {
            // if exceeding hard limit, only index non-zero grade or error result
            return result.isHighPriority();
        }
    }

    /**
     * Adds the result to a flattened index if the flattened index exists.
     */
    private void addToFlattenedIndexIfExists(BulkRequest bulkRequest, AnomalyResult result, String resultIndex) {
        String flattenedResultIndexName = resultIndex + "_flattened_" + result.getDetectorId().toLowerCase();
        System.out.println("ADResultBulkTransportAction 111: " + flattenedResultIndexName);
        if (doesFlattenedResultIndexExist(flattenedResultIndexName)) {
            System.out.println("ADResultBulkTransportAction 113: exist");
            addResult(bulkRequest, result, flattenedResultIndexName);
        }
    }

    private boolean doesFlattenedResultIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }

    private void addResult(BulkRequest bulkRequest, AnomalyResult result, String resultIndex) {
        String index = resultIndex == null ? indexName : resultIndex;
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(index).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error("Failed to prepare bulk index request for index " + index, e);
        }
    }
}
