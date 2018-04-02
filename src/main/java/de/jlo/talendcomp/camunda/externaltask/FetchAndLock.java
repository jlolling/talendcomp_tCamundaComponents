package de.jlo.talendcomp.camunda.externaltask;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FetchAndLock extends CamundaClient {
	
	private List<String> requestedVariables = new ArrayList<String>();
	private int numberTaskToFetch = 1;
	private ArrayNode fetchedTaskArray = null;
	private ObjectNode currentTask = null;
	private int currentTaskIndex = 0;
	private int secondsBetweenFetches = 1;
	private long startTime = 0;
	private long stopTime = 0;
	protected long lockDuration = 1000l;
	
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
		currentTaskIndex = 0;
		currentTask = null;
		while (true) {
			if (stopTimeReached()) {
				break;
			}
			HttpClient client = createHttpClient();
			String responseStr = client.post(getExternalTaskEndpointURL() + "/fetchAndLock", camundaUser, camundaPassword, requestPayload, true);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Response: " + responseStr);
			}
			if (client.getStatusCode() != 200) {
				String message = "FetchAndLock POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage() + "\nResponse: " + responseStr;
				LOG.error(message);
				throw new Exception(message);
			}
			try {
				fetchedTaskArray = (ArrayNode) objectMapper.readTree(responseStr);
			} catch (Exception e) {
				LOG.error("fetchAndLock failed to parse response: " + responseStr);
				throw e;
			}
			if (fetchedTaskArray.size() > 0) {
				break;
			}
			Thread.sleep(secondsBetweenFetches * 1000l);
		}
		return fetchedTaskArray.size();
	}
	
	public void addVariable(String var) {
		if (requestedVariables.contains(var) == false) {
			requestedVariables.add(var);
		}
	}
	
	private boolean stopTimeReached() {
		if (startTime > 0l && stopTime > 0l) {
			// check the runtime but take care we are trying fetch at at least one time
			long currentTime = System.currentTimeMillis();
			if (currentTime >= stopTime) {
				LOG.info("Stop fetching task because max runtime is reached.");
				return true;
			}
		}
		return false;
	}

	public boolean next() throws Exception {
		if (stopTimeReached()) {
			return false;
		}
		if (fetchedTaskArray == null) {
			startTime = System.currentTimeMillis();
			LOG.debug("Fetch and lock intial tasks");
			fetchAndLock();
		}
		if (currentTaskIndex == fetchedTaskArray.size()) {
			LOG.debug("Fetch and lock for the next tasks");
			Thread.sleep(secondsBetweenFetches * 1000l);
			fetchAndLock();
		}
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

	public int getSecondsBetweenFetches() {
		return secondsBetweenFetches;
	}

	public void setSecondsBetweenFetches(Integer secondsBetweenFetches) {
		if (secondsBetweenFetches != null && secondsBetweenFetches >= 1) {
			this.secondsBetweenFetches = secondsBetweenFetches;
		}
	}

	public long getStopTime() {
		return stopTime;
	}

	public void setStopTime(Integer secondsFetching) {
		if (secondsFetching != null) {
			this.stopTime = System.currentTimeMillis() + 10l + (secondsFetching * 1000l);
		}
	}

	public void setStopTime(Date stopDate) {
		if (stopDate != null) {
			this.stopTime = stopDate.getTime() + 10l;
		}
	}

	public int getNumberTaskToFetch() {
		return numberTaskToFetch;
	}

	public void setNumberTaskToFetch(Integer numberTaskToFetch) {
		if (numberTaskToFetch != null) {
			this.numberTaskToFetch = numberTaskToFetch;
		}
	}

	public void setLockDuration(Long lockDuration) {
		if (lockDuration != null) {
			this.lockDuration = lockDuration.longValue();
		}
	}
	
	public void setLockDuration(Integer lockDuration) {
		if (lockDuration != null) {
			this.lockDuration = lockDuration.longValue();
		}
	}

	public long getLockDuration() {
		return lockDuration;
	}

}
