package com.google.cloud.gcs.analyticscore.common.telemetry;

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
    if (!isLogLevelEnabled()) {
      return;
    }
    String message =
        String.format(
            "Operation started: [%s], id: [%s], attributes: %s",
            operation.getName(), operation.getOperationId(), operation.getAttributes());
    logMessage(message);
  }

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    if (!isLogLevelEnabled()) {
      return;
    }
    String message =
        String.format(
            "Operation ended: [%s], id: [%s], attributes: %s, metrics: %s",
            operation.getName(),
            operation.getOperationId(),
            operation.getAttributes(),
            formatMetrics(metrics));
    logMessage(message);
  }

  private boolean isLogLevelEnabled() {
    if (options == null || options.getLogLevel() == null) {
      return LOG.isInfoEnabled();
    }
    switch (options.getLogLevel()) {
      case TRACE:
        return LOG.isTraceEnabled();
      case DEBUG:
        return LOG.isDebugEnabled();
      case WARNING:
        return LOG.isWarnEnabled();
      case ERROR:
        return LOG.isErrorEnabled();
      case INFO:
      default:
        return LOG.isInfoEnabled();
    }
  }

  /**
   * Formats a map of metrics into a generic, readable string. Sample : {metric1=1, metric2=2,
   * metric3=3}
   */
  private String formatMetrics(Map<MetricKey, Long> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      MetricKey key = entry.getKey();
      sb.append(key.getName());
      if (key.getAttributes() != null && !key.getAttributes().isEmpty()) {
        sb.append(key.getAttributes());
      }
      sb.append("=").append(entry.getValue());
    }
    sb.append("}");
    return sb.toString();
  }

  private void logMessage(String message) {
    if (options == null || options.getLogLevel() == null) {
      LOG.info(message);
      return;
    }
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
