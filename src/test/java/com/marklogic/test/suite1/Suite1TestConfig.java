package com.marklogic.test.suite1;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.admin.AdminConfig;

/**
 * Defines configuration for the JUnit tests. The non-version-controlled
 * user.properties file is imported second so that a developer can override
 * what's in test.properties.
 */
@Configuration
@PropertySource(value = { "classpath:user.properties", "classpath:suite1.properties",
		"classpath:contentpump.properties",
		"classpath:DocumentLoadWithoutDatabase.properties" }, ignoreResourceNotFound = true)
public class Suite1TestConfig {

	@Value("${mlHost:localhost}")
	private String mlManageHost;

	@Value("${mlUser:admin}")
	private String mlManageUsername;

	@Value("${mlPassword:admin}")
	private String mlManagePassword;

	/**
	 * Has to be static so that Spring instantiates it first.
	 */
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
		PropertySourcesPlaceholderConfigurer c = new PropertySourcesPlaceholderConfigurer();
		c.setIgnoreResourceNotFound(true);
		return c;
	}

	@Bean
	public ManageConfig manageConfig() {
		ManageConfig config = new ManageConfig(getMlManageHost(), 8002, getMlManageUsername(), getMlManagePassword());

		// Clean the JSON by default
		config.setCleanJsonPayloads(true);
		return config;
	}

	/**
	 * For now, assume the username/password works for 8001 too. Easy to make
	 * this configurable later if needed.
	 */
	@Bean
	public AdminConfig adminConfig() {
		return new AdminConfig(getMlManageHost(), 8001, getMlManageUsername(), getMlManagePassword());
	}

	public String getMlManageHost() {
		return mlManageHost;
	}

	public String getMlManageUsername() {
		return mlManageUsername;
	}

	public String getMlManagePassword() {
		return mlManagePassword;
	}
}