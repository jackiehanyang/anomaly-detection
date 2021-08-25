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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.test.OpenSearchTestCase.buildNewFakeTransportAddress;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomDouble;
import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.feature.Features;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.model.TimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestHelpers {

    public static final String LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI = "/_opendistro/_anomaly_detection/detectors";
    public static final String AD_BASE_DETECTORS_URI = "/_plugins/_anomaly_detection/detectors";
    public static final String AD_BASE_RESULT_URI = AD_BASE_DETECTORS_URI + "/results";
    public static final String AD_BASE_PREVIEW_URI = AD_BASE_DETECTORS_URI + "/%s/_preview";
    public static final String AD_BASE_STATS_URI = "/_plugins/_anomaly_detection/stats";
    public static ImmutableSet<String> historicalAnalysisRunningStats = ImmutableSet
        .of(ADTaskState.CREATED.name(), ADTaskState.INIT.name(), ADTaskState.RUNNING.name());
    private static final Logger logger = LogManager.getLogger(TestHelpers.class);
    public static final Random random = new Random(42);

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        String jsonEntity,
        List<Header> headers
    ) throws IOException {
        HttpEntity httpEntity = Strings.isBlank(jsonEntity) ? null : new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON);
        return makeRequest(client, method, endpoint, params, httpEntity, headers);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    public static String xContentBuilderToString(XContentBuilder builder) {
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    public static XContentParser parser(String xc) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc);
        parser.nextToken();
        return parser;
    }

    public static Map<String, Object> XContentBuilderToMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    public static NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime) throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature()), uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime, boolean featureEnabled)
        throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature(featureEnabled)), uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features, Map<String, Object> uiMetadata, Instant lastUpdateTime)
        throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType
    ) throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, detectorType, true);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType,
        boolean withUser
    ) throws IOException {
        return randomAnomalyDetector(
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            features,
            uiMetadata,
            lastUpdateTime,
            detectorType,
            OpenSearchRestTestCase.randomLongBetween(1, 1000),
            withUser
        );
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<String> indices,
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType,
        long detectionIntervalInMinutes,
        boolean withUser
    ) throws IOException {
        User user = withUser ? randomUser() : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            indices,
            features,
            randomQuery(),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            randomIntervalTimeConfiguration(),
            // our test's heap allowance is very small (20 MB heap usage would cause OOM)
            // reduce size to not cause issue.
            randomIntBetween(1, 20),
            uiMetadata,
            randomInt(),
            lastUpdateTime,
            null,
            user,
            detectorType
        );
    }

    public static AnomalyDetector randomDetector(List<Feature> features, String indexName, int detectionIntervalInMinutes, String timeField)
        throws IOException {
        String detectorType = AnomalyDetectorType.SINGLE_ENTITY.name();
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            ImmutableList.of(indexName),
            features,
            randomQuery("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"value\"}}]}}"),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            8,
            null,
            randomInt(),
            Instant.now(),
            null,
            null,
            detectorType
        );
    }

    public static DetectionDateRange randomDetectionDateRange() {
        return new DetectionDateRange(
            Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(10, ChronoUnit.DAYS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetector randomAnomalyDetectorUsingCategoryFields(String detectorId, List<String> categoryFields)
        throws IOException {
        return new AnomalyDetector(
            detectorId,
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(randomFeature(true)),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            categoryFields,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            features,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithEmptyFeature() throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval) throws IOException {
        return randomAnomalyDetectorWithInterval(interval, false);
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval, boolean hcDetector) throws IOException {
        List<String> categoryField = hcDetector ? ImmutableList.of(randomAlphaOfLength(5)) : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(randomFeature()),
            randomQuery(),
            interval,
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            categoryField,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval, boolean hcDetector, boolean featureEnabled)
        throws IOException {
        List<String> categoryField = hcDetector ? ImmutableList.of(randomAlphaOfLength(5)) : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(randomFeature(featureEnabled)),
            randomQuery(),
            interval,
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            categoryField,
            randomUser()
        );
    }

    public static SearchSourceBuilder randomFeatureQuery() throws IOException {
        String query = "{\"query\":{\"match\":{\"user\":{\"query\":\"kimchy\",\"operator\":\"OR\",\"prefix_length\":0,"
            + "\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\","
            + "\"auto_generate_synonyms_phrase_query\":true,\"boost\":1}}}}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), LoggingDeprecationHandler.INSTANCE, query);
        searchSourceBuilder.parseXContent(parser);
        return searchSourceBuilder;
    }

    public static QueryBuilder randomQuery() throws IOException {
        String query = "{\"bool\":{\"must\":{\"term\":{\"user\":\"kimchy\"}},\"filter\":{\"term\":{\"tag\":"
            + "\"tech\"}},\"must_not\":{\"range\":{\"age\":{\"gte\":10,\"lte\":20}}},\"should\":[{\"term\":"
            + "{\"tag\":\"wow\"}},{\"term\":{\"tag\":\"elasticsearch\"}}],\"minimum_should_match\":1,\"boost\":1}}";
        return randomQuery(query);
    }

    public static QueryBuilder randomQuery(String query) throws IOException {
        XContentParser parser = TestHelpers.parser(query);
        return parseInnerQueryBuilder(parser);
    }

    public static AggregationBuilder randomAggregation() throws IOException {
        return randomAggregation(randomAlphaOfLength(5));
    }

    public static AggregationBuilder randomAggregation(String aggregationName) throws IOException {
        XContentParser parser = parser("{\"" + aggregationName + "\":{\"value_count\":{\"field\":\"ok\"}}}");

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    /**
     * Parse string aggregation query into {@link AggregationBuilder}
     * Sample input:
     * "{\"test\":{\"value_count\":{\"field\":\"ok\"}}}"
     *
     * @param aggregationQuery aggregation builder
     * @return aggregation builder
     * @throws IOException IO exception
     */
    public static AggregationBuilder parseAggregation(String aggregationQuery) throws IOException {
        XContentParser parser = parser(aggregationQuery);

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    public static Map<String, Object> randomUiMetadata() {
        return ImmutableMap.of(randomAlphaOfLength(5), randomFeature());
    }

    public static TimeConfiguration randomIntervalTimeConfiguration() {
        return new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 1000), ChronoUnit.MINUTES);
    }

    public static IntervalSchedule randomIntervalSchedule() {
        return new IntervalSchedule(
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            OpenSearchRestTestCase.randomIntBetween(1, 1000),
            ChronoUnit.MINUTES
        );
    }

    public static Feature randomFeature() {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public static Feature randomFeature(String featureName, String aggregationName) {
        return randomFeature(featureName, aggregationName, randomBoolean());
    }

    public static Feature randomFeature(boolean enabled) {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5), enabled);
    }

    public static Feature randomFeature(String featureName, String aggregationName, boolean enabled) {
        AggregationBuilder testAggregation = null;
        try {
            testAggregation = randomAggregation(aggregationName);
        } catch (IOException e) {
            logger.error("Fail to generate test aggregation");
            throw new RuntimeException();
        }
        return new Feature(randomAlphaOfLength(5), featureName, enabled, testAggregation);
    }

    public static Features randomFeatures() {
        List<Map.Entry<Long, Long>> ranges = Arrays.asList(new AbstractMap.SimpleEntry<>(0L, 1L));
        double[][] unprocessed = new double[][] { { randomDouble(), randomDouble() } };
        double[][] processed = new double[][] { { randomDouble(), randomDouble() } };

        return new Features(ranges, unprocessed, processed);
    }

    public static List<ThresholdingResult> randomThresholdingResults() {
        double grade = 1.;
        double confidence = 0.5;
        double score = 1.;

        ThresholdingResult thresholdingResult = new ThresholdingResult(grade, confidence, score);
        List<ThresholdingResult> results = new ArrayList<>();
        results.add(thresholdingResult);
        return results;
    }

    public static User randomUser() {
        return new User(
            randomAlphaOfLength(8),
            ImmutableList.of(randomAlphaOfLength(10)),
            ImmutableList.of("all_access"),
            ImmutableList.of("attribute=test")
        );
    }

    public static <S, T> void assertFailWith(Class<S> clazz, Callable<T> callable) throws Exception {
        assertFailWith(clazz, null, callable);
    }

    public static <S, T> void assertFailWith(Class<S> clazz, String message, Callable<T> callable) throws Exception {
        try {
            callable.call();
        } catch (Throwable e) {
            if (e.getClass() != clazz) {
                throw e;
            }
            if (message != null && !e.getMessage().contains(message)) {
                throw e;
            }
        }
    }

    public static FeatureData randomFeatureData() {
        return new FeatureData(randomAlphaOfLength(5), randomAlphaOfLength(5), randomDouble());
    }

    public static AnomalyResult randomAnomalyDetectResult() {
        return randomAnomalyDetectResult(randomDouble(), randomAlphaOfLength(5), null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score) {
        return randomAnomalyDetectResult(randomDouble(), null, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(String error) {
        return randomAnomalyDetectResult(Double.NaN, error, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId) {
        return randomAnomalyDetectResult(score, error, taskId, true);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId, boolean withUser) {
        User user = withUser ? randomUser() : null;
        return new AnomalyResult(
            randomAlphaOfLength(5),
            taskId,
            score,
            randomDouble(),
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error,
            null,
            user,
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(double score, double grade) {
        return randomHCADAnomalyDetectResult(score, grade, null);
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(double score, double grade, String error) {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            score,
            grade,
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error,
            Entity.createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            randomUser(),
            CommonValue.NO_SCHEMA_VERSION
        );
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob() {
        return randomAnomalyDetectorJob(true);
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob(boolean enabled, Instant enabledTime, Instant disabledTime) {
        return new AnomalyDetectorJob(
            randomAlphaOfLength(10),
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            enabled,
            enabledTime,
            disabledTime,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L,
            randomUser()
        );
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob(boolean enabled) {
        return randomAnomalyDetectorJob(
            enabled,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetectorExecutionInput randomAnomalyDetectorExecutionInput() throws IOException {
        return new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minus(10, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAnomalyDetector(null, Instant.now().truncatedTo(ChronoUnit.SECONDS))
        );
    }

    public static ActionListener<CreateIndexResponse> createActionListener(
        CheckedConsumer<CreateIndexResponse, ? extends Exception> consumer,
        Consumer<Exception> failureConsumer
    ) {
        return ActionListener.wrap(consumer, failureConsumer);
    }

    public static void waitForIndexCreationToComplete(Client client, final String indexName) {
        ClusterHealthResponse clusterHealthResponse = client
            .admin()
            .cluster()
            .prepareHealth(indexName)
            .setWaitForEvents(Priority.URGENT)
            .get();
        logger.info("Status of " + indexName + ": " + clusterHealthResponse.getStatus());
    }

    public static ClusterService createClusterService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            OpenSearchRestTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            BUILT_IN_ROLES,
            Version.CURRENT
        );
        return ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    public static ClusterState createIndexBlockedState(String indexName, Settings hackedSettings, String alias) {
        ClusterState blockedClusterState = null;
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        if (alias != null) {
            builder.putAlias(AliasMetadata.builder(alias));
        }
        IndexMetadata indexMetaData = builder
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(hackedSettings)
            )
            .build();
        Metadata metaData = Metadata.builder().put(indexMetaData, false).build();
        blockedClusterState = ClusterState
            .builder(new ClusterName("test cluster"))
            .metadata(metaData)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData))
            .build();
        return blockedClusterState;
    }

    public static ThreadContext createThreadContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext context = new ThreadContext(build);
        context.putHeader("foo", "bar");
        context.putTransient("x", 1);
        return context;
    }

    public static ThreadPool createThreadPool() {
        ThreadPool pool = mock(ThreadPool.class);
        when(pool.getThreadContext()).thenReturn(createThreadContext());
        return pool;
    }

    public static CreateIndexResponse createIndex(AdminClient adminClient, String indexName, String indexMapping) {
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(AnomalyDetector.TYPE, indexMapping, XContentType.JSON);
        return adminClient.indices().create(request).actionGet(5_000);
    }

    public static void createIndex(RestClient client, String indexName, HttpEntity data) throws IOException {
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "/" + indexName + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                ImmutableMap.of(),
                data,
                null
            );
    }

    public static GetResponse createGetResponse(ToXContentObject o, String id, String indexName) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        return new GetResponse(
            new GetResult(
                indexName,
                MapperService.SINGLE_MAPPING_NAME,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.bytes(content),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static GetResponse createBrokenGetResponse(String id, String indexName) throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[0];
        return new GetResponse(
            new GetResult(
                indexName,
                MapperService.SINGLE_MAPPING_NAME,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.fromByteBuffers(buffers),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static SearchResponse createSearchResponse(ToXContentObject o) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0).sourceRef(BytesReference.bytes(content));

        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static SearchResponse createEmptySearchResponse() throws IOException {
        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static DetectorInternalState randomDetectState(String error) {
        return randomDetectState(error, Instant.now());
    }

    public static DetectorInternalState randomDetectState(Instant lastUpdateTime) {
        return randomDetectState(randomAlphaOfLength(5), lastUpdateTime);
    }

    public static DetectorInternalState randomDetectState(String error, Instant lastUpdateTime) {
        return new DetectorInternalState.Builder().lastUpdateTime(lastUpdateTime).error(error).build();
    }

    public static Map<String, Map<String, Map<String, FieldMappingMetadata>>> createFieldMappings(
        String index,
        String fieldName,
        String fieldType
    ) throws IOException {
        Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata(
            fieldName,
            new BytesArray("{\"" + fieldName + "\":{\"type\":\"" + fieldType + "\"}}")
        );
        mappings.put(index, Collections.singletonMap(CommonName.MAPPING_TYPE, Collections.singletonMap(fieldName, fieldMappingMetadata)));
        return mappings;
    }

    public static ADTask randomAdTask() throws IOException {
        return randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.RUNNING,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAlphaOfLength(5),
            true
        );
    }

    public static ADTask randomAdTask(
        String taskId,
        ADTaskState state,
        Instant executionEndTime,
        String stoppedBy,
        String detectorId,
        AnomalyDetector detector
    ) {
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL_SINGLE_ENTITY.name())
            .detectorId(detectorId)
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .build();
        return task;
    }

    public static ADTask randomAdTask(String taskId, ADTaskState state, Instant executionEndTime, String stoppedBy, boolean withDetector)
        throws IOException {
        AnomalyDetector detector = withDetector
            ? randomAnomalyDetector(ImmutableMap.of(), Instant.now().truncatedTo(ChronoUnit.SECONDS), true)
            : null;
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL_SINGLE_ENTITY.name())
            .detectorId(randomAlphaOfLength(5))
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .build();
        return task;
    }

    public static ADTask randomAdTask(
        String taskId,
        ADTaskState state,
        Instant executionEndTime,
        String stoppedBy,
        AnomalyDetector detector
    ) throws IOException {
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        Entity entity = null;
        if (detector != null) {
            if (detector.isMultiCategoryDetector()) {
                Map<String, Object> attrMap = new HashMap<>();
                detector.getCategoryField().stream().forEach(f -> attrMap.put(f, randomAlphaOfLength(5)));
                entity = Entity.createEntityByReordering(attrMap);
            } else if (detector.isMultientityDetector()) {
                entity = Entity.createEntityByReordering(ImmutableMap.of(detector.getCategoryField().get(0), randomAlphaOfLength(5)));
            }
        }
        String taskType = entity == null ? ADTaskType.HISTORICAL_SINGLE_ENTITY.name() : ADTaskType.HISTORICAL_HC_ENTITY.name();
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(taskType)
            .detectorId(randomAlphaOfLength(5))
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .entity(entity)
            .build();
        return task;
    }

    public static HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), APPLICATION_JSON);
    }

    public static HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

    public static String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(object.toXContent(builder, ToXContent.EMPTY_PARAMS));
    }

    public static RestStatus restStatus(Response response) {
        return RestStatus.fromCode(response.getStatusLine().getStatusCode());
    }

    public static SearchHits createSearchHits(int totalHits) {
        List<SearchHit> hitList = new ArrayList<>();
        IntStream.range(0, totalHits).forEach(i -> hitList.add(new SearchHit(i)));
        SearchHit[] hitArray = new SearchHit[hitList.size()];
        return new SearchHits(hitList.toArray(hitArray), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0F);
    }

    public static DiscoveryNode randomDiscoveryNode() {
        return new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public static SearchRequest matchAllRequest() {
        BoolQueryBuilder query = new BoolQueryBuilder().filter(new MatchAllQueryBuilder());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        return new SearchRequest().source(searchSourceBuilder);
    }

    public static Map<String, Object> parseStatsResult(String statsResult) throws IOException {
        XContentParser parser = TestHelpers.parser(statsResult);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, Object> adStats = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (fieldName.equals("nodes")) {
                Map<String, Object> nodesAdStats = new HashMap<>();
                adStats.put("nodes", nodesAdStats);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String nodeId = parser.currentName();
                    Map<String, Object> nodeAdStats = new HashMap<>();
                    nodesAdStats.put(nodeId, nodeAdStats);
                    parser.nextToken();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String nodeStatName = parser.currentName();
                        XContentParser.Token token = parser.nextToken();
                        if (nodeStatName.equals("models")) {
                            parser.skipChildren();
                        } else if (nodeStatName.contains("_count")) {
                            nodeAdStats.put(nodeStatName, parser.longValue());
                        } else {
                            nodeAdStats.put(nodeStatName, parser.text());
                        }
                    }
                }
            } else if (fieldName.contains("_count")) {
                adStats.put(fieldName, parser.longValue());
            } else {
                adStats.put(fieldName, parser.text());
            }
        }
        return adStats;
    }
}