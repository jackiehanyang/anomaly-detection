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

package org.opensearch.ad.transport;

import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_EMPTY_DATA_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_INVALID_QUERY_MSG;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.common.unit.TimeValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ValidateAnomalyDetectorTransportActionTests extends ADIntegTestCase {

    @Test
    public void testValidateAnomalyDetectorWithNoIssue() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNull(response.getIssue());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateName() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        createDetectorIndex();
        createDetector(anomalyDetector);
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithNonExistingFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, "non_existing_field");
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_EMPTY_DATA_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_EMPTY_DATA_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithMultipleInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField);
        Feature maxFeatureTwo = maxValueFeature("test_two", categoryField);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(maxFeature, maxFeatureTwo), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(response.getIssue().getSubIssues().keySet().size(), 2);
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }
}