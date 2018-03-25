package de.jlo.talendcomp.camunda.externaltask;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CamundaClient {
	
	private static Logger LOG = LoggerFactory.getLogger(CamundaClient.class);
	private int numberTaskToFetch = 1;
	private String topicName = null;
	private long lockDuration = 1000l;
	private String workerId = null;
	private String camundaServiceEndpointURL = null;
	private boolean needAuthorization = true;
	private String camundaUser = null;
	private String camundaPassword = null;
	private int maxRetriesInCaseOfErrors = 1;
	private int timeout = 10000;
	private List<String> requestedVariables = new ArrayList<String>();
	private final static ObjectMapper objectMapper = new ObjectMapper();
	private ArrayNode fetchedTaskArray = null;
	private ObjectNode currentTask = null;
	private int currentTaskIndex = 0;
	private Locale defaultLocale = Locale.getDefault();
	private String camundaEngine = "default";
	
	private HttpClient createHttpClient() throws Exception {
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
		httpClient.setTimeout(timeout);
		httpClient.setMaxRetriesInCaseOfErrors(maxRetriesInCaseOfErrors);
		return httpClient;
	}

	public void setTopicName(String topicName) {
		if (Util.isEmpty(topicName)) {
			throw new IllegalArgumentException("Topic name cannot be null or empty");
		}
		this.topicName = topicName;
	}

	public void setLockDuration(Long lockDuration) {
		if (lockDuration != null) {
			this.lockDuration = lockDuration.longValue();
		}
	}
	
	public int getNumberTaskToFetch() {
		return numberTaskToFetch;
	}

	public void setNumberTaskToFetch(int numberTaskToFetch) {
		this.numberTaskToFetch = numberTaskToFetch;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public long getLockDuration() {
		return lockDuration;
	}

	public String getCamundaServiceEndpointURL() {
		return camundaServiceEndpointURL;
	}

	public void setCamundaServiceEndpointURL(String camundaURL) {
		if (Util.isEmpty(camundaURL)) {
			throw new IllegalArgumentException("Camunda REST service endpoint URL cannot be null or empty");
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

	private ArrayNode getVariableNames() {
		if (requestedVariables.isEmpty()) {
			throw new IllegalStateException("No topic variables available, at least one is mandatory!");
		}
		ArrayNode an = objectMapper.createArrayNode();
		for (String var : requestedVariables) {
			an.add(var);
		}
		return an;
	}
	
	private String getExternalTaskEndpointURL() {
		return camundaServiceEndpointURL + "/engine-rest/engine/" + camundaEngine + "/external-task";
	}
	
	public int fetchAndLock() throws Exception {
		if (workerId == null) {
			throw new IllegalStateException("initialize failed: workerId is not set");
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("maxTasks", numberTaskToFetch);
		requestPayload
			.withArray("topics")
				.addObject()
					.put("topicName", topicName)
					.put("lockDuration", lockDuration)
					.set("variables", getVariableNames());
		HttpClient client = createHttpClient();
		String responseStr = client.post(getExternalTaskEndpointURL() + "/fetchAndLock", camundaUser, camundaPassword, requestPayload);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Response: " + responseStr);
		}
		try {
			currentTaskIndex = 0;
			currentTask = null;
			fetchedTaskArray = (ArrayNode) objectMapper.readTree(responseStr);
		} catch (Exception e) {
			LOG.error("fetchAndLock failed to parse response: " + responseStr);
			throw e;
		}
		return fetchedTaskArray.size();
	}
	
	public void complete(String taskId, Map<String, Object> variables) throws Exception {
		
	}
	
	public void failure(String taskId, String errorMessage) throws Exception {
		
	}
	
	public void unlock(String taskId) throws Exception {
		
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
	
	public void addVariable(String var) {
		if (requestedVariables.contains(var) == false) {
			requestedVariables.add(var);
		}
	}

	public void setDefaultLocale(String localeStr) {
		if (localeStr != null && localeStr.trim().isEmpty() == false) {
			this.defaultLocale = Util.createLocale(localeStr);
		}
	}
	
	public String getTopicName() {
		return topicName;
	}
	
	public boolean nextTask() {
		if (fetchedTaskArray != null) {
			if (currentTaskIndex < fetchedTaskArray.size()) {
				currentTask = (ObjectNode) fetchedTaskArray.get(currentTaskIndex++);
				return true;
			}
		}
		return false;
	}
	
	public String getCurrentTaskId() {
		if (currentTask == null || currentTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		String id = currentTask.get("id").asText();
		if (Util.isEmpty(id)) {
			throw new IllegalStateException("Task without id received. Current task: " + currentTask.toString());
		}
		return id;
	}
	
	public String getCurrentTaskProcessInstanceId() {
		if (currentTask == null || currentTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		String id = currentTask.get("processInstanceId").asText();
		if (Util.isEmpty(id)) {
			throw new IllegalStateException("Task without processInstanceId received. Current task: " + currentTask.toString());
		}
		return id;
	}

	public JsonNode getCurrentTaskVariableValueAsObject(String varName) {
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("Variable name cannot be null or empty");
		}
		if (currentTask == null || currentTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		JsonNode varNode = currentTask.get("variables");
		if (varNode instanceof ObjectNode) {
			JsonNode valueNode = varNode.path(varName);
			if (valueNode.isNull() || valueNode.isMissingNode()) {
				return null;
			} else {
				return valueNode;
			}
		} else {
			throw new IllegalStateException("Task without variables received. Current task: " + currentTask.toString());
		}
	}
	
	public String getCurrentTaskVariableValueAsString(String varName) {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return valueNode.asText();
		} else {
			return null;
		}
	}

	public Short getCurrentTaskVariableValueAsShort(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToShort(valueNode);
		} else {
			return null;
		}
	}

	public Integer getCurrentTaskVariableValueAsInteger(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToInteger(valueNode);
		} else {
			return null;
		}
	}

	public Long getCurrentTaskVariableValueAsLong(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToLong(valueNode);
		} else {
			return null;
		}
	}

	public Double getCurrentTaskVariableValueAsDouble(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToDouble(valueNode);
		} else {
			return null;
		}
	}

	public Float getCurrentTaskVariableValueAsFloat(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToFloat(valueNode);
		} else {
			return null;
		}
	}

	public BigDecimal getCurrentTaskVariableValueAsBigDecimal(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToBigDecimal(valueNode);
		} else {
			return null;
		}
	}

	public Boolean getCurrentTaskVariableValueAsBoolean(String varName) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return TypeUtil.convertToBoolean(valueNode);
		} else {
			return null;
		}
	}

	public Date getCurrentTaskVariableValueAsDate(String varName, String pattern) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueAsObject(varName);
		if (valueNode != null) {
			return parseDate(valueNode.asText(), pattern);
		} else {
			return null;
		}
	}

	private Date parseDate(String value, String pattern) throws ParseException {
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

}
