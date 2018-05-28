package de.jlo.talendcomp.camunda.externaltask;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.jlo.talendcomp.camunda.CamundaClient;
import de.jlo.talendcomp.camunda.HttpClient;
import de.jlo.talendcomp.camunda.TypeUtil;
import de.jlo.talendcomp.camunda.Util;

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
	private long numberFetches = 0;
	private int numberSucessfulFetches = 0;
	private int numberTasksReceived = 0;
	private boolean firstFetch = true;
	protected String workerId = null;
	protected String topicName = null;
	private boolean usePriority = false;
	
	public FetchAndLock() {
		startTime = System.currentTimeMillis();
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
	
	public int fetchAndLock() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("################ fetch and lock #################");
		}
		if (workerId == null) {
			throw new IllegalStateException("initialize failed: workerId is not set");
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("maxTasks", numberTaskToFetch);
		requestPayload.put("usePriority", usePriority);
		requestPayload
			.withArray("topics")
				.addObject()
					.put("topicName", topicName)
					.put("lockDuration", lockDuration)
					.set("variables", getVariableNames());
		currentTaskIndex = 0;
		fetchedTaskArray = null;
		while (true) {
			if (stopTimeReached()) {
				break;
			}
			if (firstFetch == false) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Wait " + secondsBetweenFetches + "s");
				}
				Thread.sleep(secondsBetweenFetches * 1000l);
			} else {
				firstFetch = false;
			}
			HttpClient client = getHttpClient();
			String responseStr = client.post(getExternalTaskEndpointURL() + "/fetchAndLock", requestPayload, true);
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
			numberFetches++;
			if (fetchedTaskArray.size() > 0) {
				numberSucessfulFetches++;
				numberTasksReceived = numberTasksReceived + fetchedTaskArray.size();
				break;
			}
		}
		if (fetchedTaskArray != null) {
			return fetchedTaskArray.size();
		} else {
			return 0;
		}
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
		currentTask = null;
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		if (fetchedTaskArray == null || currentTaskIndex == fetchedTaskArray.size()) {
			fetchAndLock();
		}
		if (fetchedTaskArray != null) {
			if (currentTaskIndex < fetchedTaskArray.size()) {
				currentTask = (ObjectNode) fetchedTaskArray.get(currentTaskIndex++);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Current task: " + currentTask);
				}
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
	
	public Date getCurrentTaskLockExpirationTime() throws Exception {
		if (currentTask == null || currentTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		String s = currentTask.path("lockExpirationTime").asText();
		if (s != null && s.trim().isEmpty() == false) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			Date time = null;
			try {
				time = sdf.parse(s);
			} catch (ParseException e) {
				String message = "Parse lockExpirationTime failed. Value: " + s + " error: " + e.getMessage();
				LOG.warn(message, e);
				return null;
			}
			return time;
		} else {
			return null;
		}
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

	private JsonNode getCurrentTaskVariableValueNode(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("Variable name cannot be null or empty");
		}
		if (currentTask == null || currentTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		JsonNode varNode = currentTask.get("variables");
		if (varNode instanceof ObjectNode) {
			JsonNode valueNode = varNode.path(varName).path("value");
			if (missingAllowed == false && valueNode.isMissingNode()) {
				throw new Exception("The variable: " + varName + " is missing. Variables node: " + varNode.toString());
			}
			if (valueNode.isNull()) {
				if (nullable == false) {
					throw new Exception("The variable: " + varName + " is null. Variables node: " + varNode.toString());
				}
				return null;
			} else {
				return valueNode;
			}
		} else {
			throw new IllegalStateException("Task without variables received. Current task: " + currentTask.toString());
		}
	}
	
	public JsonNode getCurrentTaskVariableValueAsObject(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			String nodeStr = valueNode.asText();
			try {
				return objectMapper.readTree(nodeStr);
			} catch (Exception e) {
				String message = "Parse variable: " + varName + " as JSON failed: " + e.getMessage() + " content: " + nodeStr;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public String getCurrentTaskVariableValueAsString(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			return valueNode.asText();
		} else {
			return null;
		}
	}

	public Short getCurrentTaskVariableValueAsShort(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToShort(valueNode);
			} catch (Exception e) {
				String message = "Convert variable: " + varName + " to Short failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Integer getCurrentTaskVariableValueAsInteger(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToInteger(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Integer failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Long getCurrentTaskVariableValueAsLong(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToLong(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Long failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Double getCurrentTaskVariableValueAsDouble(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToDouble(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Double failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Float getCurrentTaskVariableValueAsFloat(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToFloat(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Float failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public BigDecimal getCurrentTaskVariableValueAsBigDecimal(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToBigDecimal(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to BigDecimal failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Boolean getCurrentTaskVariableValueAsBoolean(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToBoolean(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Boolean failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Date getCurrentTaskVariableValueAsDate(String varName, String pattern, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return parseDate(valueNode.asText(), pattern);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Date failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
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

	public ObjectNode getCurrentTask() {
		return currentTask;
	}

	public long getNumberFetches() {
		return numberFetches;
	}

	public int getNumberSucessfulFetches() {
		return numberSucessfulFetches;
	}

	public int getNumberTasksReceived() {
		return numberTasksReceived;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public void setTopicName(String topicName) {
		if (Util.isEmpty(topicName)) {
			throw new IllegalArgumentException("Topic name cannot be null or empty");
		}
		this.topicName = topicName;
	}

	public String getTopicName() {
		return topicName;
	}

	public boolean isUsePriority() {
		return usePriority;
	}

	public void setUsePriority(Boolean usePriority) {
		if (usePriority != null) {
			this.usePriority = usePriority.booleanValue();
		}
	}
	
}
