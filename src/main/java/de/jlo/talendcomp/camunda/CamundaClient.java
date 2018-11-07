package de.jlo.talendcomp.camunda;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class CamundaClient {
	
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
	private HttpClient cachedHttpClient = null;
	private Object lock = new Object();
	
	public void setHttpClient(HttpClient cachedHttpClient) {
		this.cachedHttpClient = cachedHttpClient;
	}
	
	public HttpClient getHttpClient() throws Exception {
		synchronized (lock) {
			if (cachedHttpClient != null) {
				return cachedHttpClient;
			} else {
				HttpClient httpClient = new HttpClient(camundaServiceEndpointURL, camundaUser, camundaPassword, timeout);
				httpClient.setMaxRetriesInCaseOfErrors(maxRetriesInCaseOfErrors);
				httpClient.setWaitMillisAfterError(waitMillisAfterError);
				cachedHttpClient = httpClient;
				return httpClient;
			}
		}
	}

	public String getCamundaServiceURL() {
		return camundaServiceEndpointURL;
	}

	public void setCamundaServiceURL(String camundaURL) {
		if (Util.isEmpty(camundaURL) == false) {
			if (camundaURL.endsWith("/")) {
				camundaURL = camundaURL.substring(0, camundaURL.length());
			}
			this.camundaServiceEndpointURL = camundaURL;
		}
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
	
	protected String getProcessDefinitionEndPoint() {
		if (alternativeEndpoint != null) {
			return alternativeEndpoint;
		} else if (camundaServiceEndpointURL != null) {
			return camundaServiceEndpointURL + "/engine-rest/engine/" + camundaEngine + "/process-definition/key/";
		} else {
			throw new IllegalStateException("Neither camundaServiceEndpointURL or alternativeEndpoint is set.");
		}
	}

	protected String getExternalTaskEndpointURL() {
		if (alternativeEndpoint != null) {
			return alternativeEndpoint;
		} else if (camundaServiceEndpointURL != null) {
			return camundaServiceEndpointURL + "/engine-rest/engine/" + camundaEngine + "/external-task";
		} else {
			throw new IllegalStateException("Neither camundaServiceEndpointURL or alternativeEndpoint is set.");
		}
	}
	
	protected String getProcessVariablesEndpointURL(String processInstanceId) {
		String extTaskURL = getExternalTaskEndpointURL();
		String endpointVariable = "/process-instance/" + processInstanceId + "/variables";
		return extTaskURL.replace("/external-task", endpointVariable);
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
	
	public String getAlternateEndpoint() {
		return alternativeEndpoint;
	}

	public void setAlternateEndpoint(String alternativeEndpoint) {
		if (Util.isEmpty(alternativeEndpoint) == false) {
			this.alternativeEndpoint = alternativeEndpoint.trim();
		}
	}

	public void close() {
		if (cachedHttpClient != null) {
			cachedHttpClient.close();
		}
	}
	
	public ObjectNode addVariableNode(ObjectNode requestNode, String varName, Object value, String pattern, String dataObjectTypName, String type) {
		if (requestNode == null) {
			throw new IllegalArgumentException("Request node cannot be null");
		}
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("Name of the variable cannot be null or empty. Parent node: " + requestNode.toString());
		}
		ObjectNode varNode = requestNode.with(varName);
		if (value != null) {
			if (value instanceof Date) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String strValue = sdf.format((Date) value);
				varNode.put("value", strValue);
				varNode.put("type", "date");
			} else if (value instanceof Short) {
				varNode.put("value", (Short) value);
				varNode.put("type", "short");
			} else if (value instanceof Integer) {
				varNode.put("value", (Integer) value);
				varNode.put("type", "integer");
			} else if (value instanceof Long) {
				varNode.put("value", (Long) value);
				varNode.put("type", "long");
			} else if (value instanceof Double) {
				varNode.put("value", (Double) value);
				varNode.put("type", "double");
			} else if (value instanceof Float) {
				varNode.put("value", (Float) value);
				varNode.put("type", "double");
			} else if (value instanceof BigDecimal) {
				varNode.put("value", (BigDecimal) value);
				varNode.put("type", "double");
			} else if (value instanceof Boolean) {
				varNode.put("value", (Boolean) value);
				varNode.put("type", "boolean");
			} else if (value instanceof byte[]) {
				varNode.put("value", (byte[]) value);
				varNode.put("type", "bytes");
			} else if (value instanceof String) {
				varNode.put("value", (String) value);
				if ("json".equals(type)) {
					if (Util.isEmpty(dataObjectTypName) == false) {
						varNode.put("type", "object");
						setValueInfo(varNode, dataObjectTypName, "application/json");
					} else {
						varNode.put("type", "json");
					}
				} else if ("xml".equals(type)) {
					if (Util.isEmpty(dataObjectTypName) == false) {
						varNode.put("type", "object");
						setValueInfo(varNode, dataObjectTypName, "application/xml");
					} else {
						varNode.put("type", "xml");
					}
				} else {
					varNode.put("type", "string");
				}
			} else if (value instanceof JsonNode) {
				varNode.put("value", value.toString());
				if ("json".equals(type)) {
					if (Util.isEmpty(dataObjectTypName) == false) {
						varNode.put("type", "object");
						setValueInfo(varNode, dataObjectTypName, "application/json");
					} else {
						varNode.put("type", "json");
					}
				} else if ("xml".equals(type)) {
					throw new IllegalArgumentException("Cannot use xml serialization if a value is a JsonNode!");
				}
			} else if (value instanceof Object) {
				varNode.put("value", value.toString());
				if ("json".equals(type)) {
					if (Util.isEmpty(dataObjectTypName) == false) {
						varNode.put("type", "object");
						setValueInfo(varNode, dataObjectTypName, "application/json");
					} else {
						varNode.put("type", "json");
					}
				} else if ("xml".equals(type)) {
					if (Util.isEmpty(dataObjectTypName) == false) {
						varNode.put("type", "object");
						setValueInfo(varNode, dataObjectTypName, "application/xml");
					} else {
						varNode.put("type", "xml");
					}
				} else {
					varNode.put("type", "string");
				}
			} else {
				varNode.put("value", value.toString());
			}
		} else {
			varNode.set("value", null);
			varNode.put("type", "null");
		}
		return requestNode;
	}
	
	/**
	 * Setup a valueInfo node for a variable node
	 * @param variable
	 * @param dataObjectTypeName
	 * @param serializationDataFormat
	 * @return the variable node
	 */
	public ObjectNode setValueInfo(ObjectNode variable, String dataObjectTypeName, String mimetype) {
		if (variable != null && dataObjectTypeName != null) {
			ObjectNode valueInfoNode = objectMapper.createObjectNode();
			valueInfoNode.put("objectTypeName", dataObjectTypeName);
			valueInfoNode.put("serializationDataFormat", mimetype);
			variable.set("valueInfo", valueInfoNode);
			return variable;
		} else {
			return null;
		}
	}

}
