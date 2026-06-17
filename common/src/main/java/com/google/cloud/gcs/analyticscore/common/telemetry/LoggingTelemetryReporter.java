/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.common.telemetry;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A telemetry reporter that logs operations and their metrics using SLF4J. The format and log level
 * are customizable through {@link LoggingTelemetryOptions}.
 */
public class LoggingTelemetryReporter implements OperationListener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingTelemetryReporter.class);

  private final LoggingTelemetryOptions options;

  public LoggingTelemetryReporter(LoggingTelemetryOptions options) {
    this.options = options;
  }

  @Override
  public void onOperationStart(Operation operation) {
    StringBuilder sb = new StringBuilder("LoggingTelemetryReporter: {");
    sb.append("\"name\":\"").append(escapeJson(operation.getName())).append("\",");
    sb.append("\"id\":\"").append(escapeJson(operation.getOperationId())).append("\",");
    sb.append("\"attributes\":").append(mapToJson(operation.getAttributes()));
    sb.append("}");
    logMessage(sb.toString());
  }

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    String escapedName = escapeJson(operation.getName());
    String escapedId = escapeJson(operation.getOperationId());
    String attributesJson = mapToJson(operation.getAttributes());

    if (metrics == null || metrics.isEmpty()) {
      StringBuilder sb = new StringBuilder("LoggingTelemetryReporter: {");
      sb.append("\"name\":\"").append(escapedName).append("\",");
      sb.append("\"id\":\"").append(escapedId).append("\",");
      sb.append("\"attributes\":").append(attributesJson);
      sb.append("}");
      logMessage(sb.toString());
      return;
    }

    for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
      StringBuilder sb = new StringBuilder("LoggingTelemetryReporter: {");
      sb.append("\"name\":\"").append(escapedName).append("\",");
      sb.append("\"id\":\"").append(escapedId).append("\",");
      sb.append("\"attributes\":").append(attributesJson).append(",");

      MetricKey key = entry.getKey();
      sb.append("\"metric_name\":\"").append(escapeJson(key.getMetric().getName())).append("\",");
      sb.append("\"metric_value\":").append(entry.getValue());
      if (key.getAttributes() != null && !key.getAttributes().isEmpty()) {
        sb.append(",\"metric_attributes\":").append(mapToJson(key.getAttributes()));
      }
      sb.append("}");
      logMessage(sb.toString());
    }
  }

  @VisibleForTesting
  String formatMetrics(Map<MetricKey, Long> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return "[]";
    }
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      first = false;
      MetricKey key = entry.getKey();
      sb.append("{");
      sb.append("\"name\":\"").append(escapeJson(key.getMetric().getName())).append("\",");
      sb.append("\"value\":").append(entry.getValue());
      if (key.getAttributes() != null && !key.getAttributes().isEmpty()) {
        sb.append(",\"attributes\":").append(mapToJson(key.getAttributes()));
      }
      sb.append("}");
    }
    sb.append("]");
    return sb.toString();
  }

  private String mapToJson(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      first = false;
      sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
      sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
    }
    sb.append("}");
    return sb.toString();
  }

  private String escapeJson(String str) {
    if (str == null) {
      return "";
    }
    return str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  private void logMessage(String message) {
    switch (options.getLogLevel()) {
      case TRACE:
        LOG.trace(message);
        break;
      case DEBUG:
        LOG.debug(message);
        break;
      case WARNING:
        LOG.warn(message);
        break;
      case ERROR:
        LOG.error(message);
        break;
      case INFO:
      default:
        LOG.info(message);
        break;
    }
  }
}
