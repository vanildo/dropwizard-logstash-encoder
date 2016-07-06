package com.wikia.dropwizard.logstash.appender;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonTypeName;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.net.SyslogConstants;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import net.logstash.logback.appender.LogstashSocketAppender;

@JsonTypeName("logstash-socket")
public class LogstashSocketAppenderFactory extends AbstractLogstashAppenderFactory {
	public LogstashSocketAppenderFactory() {
		port = SyslogConstants.SYSLOG_PORT;
	}

	@Override
	public Appender<ILoggingEvent> build(LoggerContext context, String applicationName,
			LayoutFactory<ILoggingEvent> layoutFactory, LevelFilterFactory<ILoggingEvent> levelFilterFactory,
			AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {

		final LogstashSocketAppender appender = new LogstashSocketAppender();

		appender.setName("logstash-socket-appender");
		appender.setContext(context);
		appender.setHost(host);
		appender.setPort(port);

		appender.setIncludeCallerData(includeCallerInfo);
		appender.setIncludeContext(includeContext);
		appender.setIncludeMdc(includeMdc);

		// novo
		appender.addFilter(levelFilterFactory.build(threshold));
		getFilterFactories().stream().forEach(f -> appender.addFilter(f.build()));

		if (customFields == null) {
			customFields = new HashMap<>();
		}

		if (prefix != null) {
			appender.setPrefix(LogstashAppenderFactoryHelper.createPatternLayoutWithContext(prefix, context));
		}

		if (suffix != null) {
			appender.setSuffix(LogstashAppenderFactoryHelper.createPatternLayoutWithContext(suffix, context));
		}

		customFields.putIfAbsent("applicationName", applicationName);
		appender.setCustomFields(LogstashAppenderFactoryHelper.getCustomFieldsFromHashMap(customFields));

		if (fieldNames != null) {
			appender.setFieldNames(LogstashAppenderFactoryHelper.getFieldNamesFromHashMap(fieldNames));
		}

		appender.start();

		return wrapAsync(appender, asyncAppenderFactory);
	}
}
