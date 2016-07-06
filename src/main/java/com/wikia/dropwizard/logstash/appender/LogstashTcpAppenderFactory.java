package com.wikia.dropwizard.logstash.appender;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import net.logstash.logback.appender.LogstashTcpSocketAppender;
import net.logstash.logback.encoder.LogstashEncoder;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.HashMap;

@JsonTypeName("logstash-tcp")
public class LogstashTcpAppenderFactory extends AbstractLogstashAppenderFactory {
  private boolean includeCallerData = false;

  @Min(1)
  @Max(65535)
  private int queueSize = LogstashTcpSocketAppender.DEFAULT_QUEUE_SIZE;

  public LogstashTcpAppenderFactory() {
    this.port = LogstashTcpSocketAppender.DEFAULT_PORT;
  }

  @JsonProperty
  public void setIncludeCallerData(boolean includeCallerData) {
    this.includeCallerData = includeCallerData;
  }

  @JsonProperty
  public boolean getIncludeCallerData() {
    return includeCallerData;
  }

  @JsonProperty
  public int getQueueSize() {
    return queueSize;
  }

  @JsonProperty
  public void setQueueSize(int queueSize) {
    this.queueSize = queueSize;
  }

  public Appender<ILoggingEvent> build(LoggerContext context, String applicationName, Layout<ILoggingEvent> layout) {
    final LogstashTcpSocketAppender appender = new LogstashTcpSocketAppender();
    final LogstashEncoder encoder = new LogstashEncoder();

    appender.setName("logstash-tcp-appender");
    appender.setContext(context);
    appender.addDestination(host + ":" + Integer.toString(port));
    appender.setIncludeCallerData(includeCallerData);
    appender.setQueueSize(queueSize);

    encoder.setIncludeCallerData(includeCallerInfo);
    encoder.setIncludeContext(includeContext);
    encoder.setIncludeMdc(includeMdc);

    if (customFields == null) {
      customFields = new HashMap<>();
    }

    customFields.putIfAbsent("applicationName", applicationName);
    encoder.setCustomFields(LogstashAppenderFactoryHelper.getCustomFieldsFromHashMap(customFields));

    if (fieldNames != null) {
      encoder.setFieldNames(LogstashAppenderFactoryHelper.getFieldNamesFromHashMap(fieldNames));
    }
//
//    appender.setEncoder(encoder);
//    encoder.start();
//    appender.start();
//
//    return wrapAsync(appender);
    return null;
  }

	@Override
	public Appender<ILoggingEvent> build(LoggerContext context, String applicationName, LayoutFactory<ILoggingEvent> layoutFactory,
		LevelFilterFactory<ILoggingEvent> levelFilterFactory, AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {
		
		final LogstashTcpSocketAppender appender = new LogstashTcpSocketAppender();
	    final LogstashEncoder encoder = new LogstashEncoder();

	    appender.setName("logstash-tcp-appender");
	    appender.setContext(context);
	    appender.addDestination(host + ":" + Integer.toString(port));
	    appender.setIncludeCallerData(includeCallerData);
	    appender.setQueueSize(queueSize);
	    //novo
	    appender.addFilter(levelFilterFactory.build(threshold));
	    getFilterFactories().stream().forEach(f -> appender.addFilter(f.build()));

	    encoder.setIncludeCallerData(includeCallerInfo);
	    encoder.setIncludeContext(includeContext);
	    encoder.setIncludeMdc(includeMdc);

	    if (customFields == null) {
	      customFields = new HashMap<>();
	    }

	    customFields.putIfAbsent("applicationName", applicationName);
	    encoder.setCustomFields(LogstashAppenderFactoryHelper.getCustomFieldsFromHashMap(customFields));

	    if (fieldNames != null) {
	      encoder.setFieldNames(LogstashAppenderFactoryHelper.getFieldNamesFromHashMap(fieldNames));
	    }
	    
		appender.setEncoder(encoder);
		encoder.start();
		appender.start();

		return wrapAsync(appender, asyncAppenderFactory);
	}
}
