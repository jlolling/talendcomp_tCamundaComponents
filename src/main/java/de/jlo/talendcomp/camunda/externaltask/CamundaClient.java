package de.jlo.talendcomp.camunda.externaltask;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CamundaClient {
	
	protected static Logger LOG = Logger.getLogger(CamundaClient.class);
	protected String topicName = null;
	protected String workerId = null;
	private String camundaServiceEndpointURL = null;
	private String alternativeEndpoint = null;
	private boolean needAuthorization = true;
	protected String camundaUser = null;
	protected String camundaPassword = null;
	private int maxRetriesInCaseOfErrors = 1;
	private long waitMillisAfterError = 1000l;
	private int timeout = 10000;
	protected final static ObjectMapper objectMapper = new ObjectMapper();
	private Locale defaultLocale = Locale.ENGLISH;
	private String camundaEngine = "default";
	
	protected HttpClient createHttpClient() throws Exception {
		if (Util.isEmpty(camundaServiceEndpointURL)) {
			throw new IllegalStateException("Camunda REST service endpoint URL not set");
		}
		if (needAuthorization) {
			if (Util.isEmpty(camundaUser)) {
				throw new IllegalStateException("User not set");
			}
			if (Util.isEmpty(camundaPassword)) {
				throw new IllegalStateException("Password not set");
			}
		}
		HttpClient httpClient = new HttpClient();
		httpClient.setDebug(isDebug());
		httpClient.setTimeout(timeout);
		httpClient.setMaxRetriesInCaseOfErrors(maxRetriesInCaseOfErrors);
		httpClient.setWaitMillisAfterError(waitMillisAfterError);
		return httpClient;
	}

	public void setTopicName(String topicName) {
		if (Util.isEmpty(topicName)) {
			throw new IllegalArgumentException("Topic name cannot be null or empty");
		}
		this.topicName = topicName;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public String getCamundaServiceURL() {
		return camundaServiceEndpointURL;
	}

	public void setCamundaServiceURL(String camundaURL) {
		if (Util.isEmpty(camundaURL)) {
			throw new IllegalArgumentException("Camunda service URL cannot be null or empty");
		}
		if (camundaURL.endsWith("/")) {
			camundaURL = camundaURL.substring(0, camundaURL.length());
		}
		this.camundaServiceEndpointURL = camundaURL;
	}

	public String getCamundaUser() {
		return camundaUser;
	}

	public void setCamundaUser(String camundaUser) {
		this.needAuthorization = true;
		this.camundaUser = camundaUser;
	}

	public String getCamundaPassword() {
		return camundaPassword;
	}

	public void setCamundaPassword(String camundaPassword) {
		this.camundaPassword = camundaPassword;
	}

	protected String getExternalTaskEndpointURL() {
		if (alternativeEndpoint != null) {
			return alternativeEndpoint;
		} else {
			return camundaServiceEndpointURL + "/engine-rest/engine/" + camundaEngine + "/external-task";
		}
	}
	
	public boolean isNeedAuthorization() {
		return needAuthorization;
	}

	public int getMaxRetriesInCaseOfErrors() {
		return maxRetriesInCaseOfErrors;
	}

	public void setMaxRetriesInCaseOfErrors(Integer maxRetriesInCaseOfErrors) {
		if (maxRetriesInCaseOfErrors != null) {
			this.maxRetriesInCaseOfErrors = maxRetriesInCaseOfErrors;
		}
	}

	public void setNeedAuthorization(boolean needAuthorization) {
		this.needAuthorization = needAuthorization;
	}
	
	public void setDefaultLocale(String localeStr) {
		if (localeStr != null && localeStr.trim().isEmpty() == false) {
			this.defaultLocale = Util.createLocale(localeStr);
		}
	}
	
	public String getTopicName() {
		return topicName;
	}
	
	protected Date parseDate(String value, String pattern) throws ParseException {
		if (value == null) {
			return null;
		}
		return GenericDateUtil.parseDate(value, defaultLocale, pattern);
	}

	public String getCamundaEngine() {
		return camundaEngine;
	}

	public void setCamundaEngine(String camundaEngine) {
		if (Util.isEmpty(camundaEngine) == false) {
			this.camundaEngine = camundaEngine;
		}
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(Integer timeout) {
		if (timeout != null) {
			this.timeout = timeout;
		}
	}

	public void setTimeout(Number timeout) {
		if (timeout != null) {
			this.timeout = timeout.intValue();
		}
	}

	public long getWaitMillisAfterError() {
		return waitMillisAfterError;
	}

	public void setWaitMillisAfterError(Long waitMillisAfterError) {
		if (waitMillisAfterError != null) {
			this.waitMillisAfterError = waitMillisAfterError.longValue();
		}
	}

	public void setWaitMillisAfterError(Integer waitMillisAfterError) {
		if (waitMillisAfterError != null) {
			this.waitMillisAfterError = waitMillisAfterError.longValue();
		}
	}
	
	public boolean isDebug() {
		return LOG.isDebugEnabled();
	}
	
	public void setDebug(Boolean debug) {
		if (debug != null) {
			if (debug == true) {
				LOG.setLevel(Level.DEBUG);
			} else {
				LOG.setLevel(Level.INFO);
			}
		}
	}

	public String getAlternateEndpoint() {
		return alternativeEndpoint;
	}

	public void setAlternateEndpoint(String alternativeEndpoint) {
		if (Util.isEmpty(alternativeEndpoint) == false) {
			this.alternativeEndpoint = alternativeEndpoint.trim();
		}
	}

}
