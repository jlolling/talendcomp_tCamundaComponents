package de.jlo.talendcomp.camunda.process;

import java.math.BigDecimal;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.jlo.talendcomp.camunda.CamundaClient;
import de.jlo.talendcomp.camunda.HttpClient;
import de.jlo.talendcomp.camunda.TypeUtil;
import de.jlo.talendcomp.camunda.Util;

public class ProcessInstance extends CamundaClient {

	private ObjectNode currentRequestVariablesNode = null;
	private ObjectNode currentResponseNode = null;
	private String processDefinitionKey = null;
	private boolean withVariablesInReturn = false;
	private String businessKey = null;
	
	public void addStartVariable(String varName, Object value, String dataObjectTypName, String type) {
		if (currentRequestVariablesNode == null) {
			currentRequestVariablesNode = objectMapper.createObjectNode();
		}
		addVariableNode(currentRequestVariablesNode, varName, value, "yyyy-MM-dd'T'HH:mm:ss", dataObjectTypName, type);
	}

	private JsonNode getCurrentResponseVariableValueNode(String varName, boolean nullable) throws Exception {
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("Variable name cannot be null or empty");
		}
		checkResponseAvailable();
		JsonNode varNode = currentResponseNode.get("variables");
		if (varNode instanceof ObjectNode) {
			JsonNode valueNode = varNode.path(varName).path("value");
			if (valueNode.isNull() || valueNode.isMissingNode()) {
				if (nullable == false) {
					throw new Exception("The variable: " + varName + " is null. Variables node: " + varNode.toString());
				} else {
					return null;
				}
			} else {
				return valueNode;
			}
		} else {
			return null;
		}
	}

	public JsonNode getCurrentResponseVariableValueAsObject(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public String getCurrentResponseVariableValueAsString(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
		if (valueNode != null) {
			return valueNode.asText();
		} else {
			return null;
		}
	}

	public Short getCurrentResponseVariableValueAsShort(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Integer getCurrentResponseVariableValueAsInteger(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Long getCurrentResponseVariableValueAsLong(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Double getCurrentResponseVariableValueAsDouble(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Float getCurrentResponseVariableValueAsFloat(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public BigDecimal getCurrentResponseVariableValueAsBigDecimal(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Boolean getCurrentResponseVariableValueAsBoolean(String varName, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public Date getCurrentResponseVariableValueAsDate(String varName, String pattern, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentResponseVariableValueNode(varName, nullable);
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

	public void start() throws Exception {
		currentResponseNode = null; // take care we do not use the former response
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("################ Start Process Instance #################");
//		}
		if (processDefinitionKey == null) {
			throw new IllegalStateException("initialize failed: processDefinitionKey is not set");
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		if (businessKey != null) {
			requestPayload.put("businessKey", businessKey);
		}
		if (currentRequestVariablesNode != null) {
			requestPayload.set("variables", currentRequestVariablesNode);
		}
		if (withVariablesInReturn) {
			requestPayload.put("withVariablesInReturn", true);
		}
		HttpClient client = getHttpClient();
		String responseStr = client.post(getProcessDefinitionEndPoint() + processDefinitionKey + "/start", requestPayload, true);
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("Response: " + responseStr);
//		}
		if (client.getStatusCode() != 200) {
			String message = "Start Process Instance POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage() + "\nResponse: " + responseStr;
//			LOG.error(message);
			throw new Exception(message);
		}
		currentRequestVariablesNode = null; // take care we do not use the same variables for the next request
		try {
			currentResponseNode = (ObjectNode) objectMapper.readTree(responseStr);
		} catch (Exception e) {
//			LOG.error("Start Process Instance failed to parse response: " + responseStr);
			throw new Exception("Start process failed: Parsing response failed: response: " + responseStr, e);
		}
	}
	
	private void checkResponseAvailable() throws Exception {
		if (currentResponseNode == null || currentResponseNode.isNull()) {
			throw new IllegalStateException("No response available");
		}
	}
	
	public String getProcessInstanceId() throws Exception {
		checkResponseAvailable();
		JsonNode node = currentResponseNode.get("id");
		if (node != null) {
			return node.asText();
		} else {
			return null;
		}
	}

	public String getBusinessKey() throws Exception {
		checkResponseAvailable();
		JsonNode node = currentResponseNode.get("businessKey");
		if (node != null) {
			return node.asText();
		} else {
			return null;
		}
	}

	public Boolean getProcessInstanceEnded() throws Exception {
		checkResponseAvailable();
		JsonNode node = currentResponseNode.get("ended");
		if (node != null) {
			return node.asBoolean();
		} else {
			return null;
		}
	}

	public Boolean getProcessInstanceSuspended() throws Exception {
		checkResponseAvailable();
		JsonNode node = currentResponseNode.get("suspended");
		if (node != null) {
			return node.asBoolean();
		} else {
			return null;
		}
	}

	public String getProcessInstanceLinks() throws Exception {
		checkResponseAvailable();
		JsonNode node = currentResponseNode.get("links");
		if (node != null) {
			return node.toString();
		} else {
			return null;
		}
	}

	public String getProcessDefinitionKey() {
		return processDefinitionKey;
	}

	public void setProcessDefinitionKey(String processDefinitionKey) {
		if (Util.isEmpty(processDefinitionKey)) {
			throw new IllegalArgumentException("Process definition key cannot be null or empty!");
		}
		this.processDefinitionKey = processDefinitionKey;
	}

	public boolean isWithVariablesInReturn() {
		return withVariablesInReturn;
	}

	public void setWithVariablesInReturn(Boolean withVariablesInReturn) {
		if (withVariablesInReturn != null) {
			this.withVariablesInReturn = withVariablesInReturn;
		}
	}

	public void setBusinessKey(String currentBusinessKey) {
		if (Util.isEmpty(currentBusinessKey) == false) {
			this.businessKey = currentBusinessKey;
		}
	}
	
}
