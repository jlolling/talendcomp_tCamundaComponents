package de.jlo.talendcomp.camunda.externaltask;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

public class HttpClient {
	
	private static Logger LOG = Logger.getLogger(HttpClient.class);
	private int connectionTimeout = 1000;
	private int statusCode = 0;
	private String statusMessage = null;
	private int maxRetriesInCaseOfErrors = 0;
	private int currentAttempt = 0;
	private long waitMillisAfterError = 1000l;
	private boolean cacheClient = false;
	private CloseableHttpClient cachedHttpClient = null;
	
	private HttpEntity buildEntity(JsonNode node) throws UnsupportedEncodingException {
		if (node != null && node.isNull() == false && node.isMissingNode() == false) {
			HttpEntity entity = new StringEntity(node.toString(), "UTF-8");
			return entity;
		} else {
			return null;
		}
	}

	private String execute(CloseableHttpClient httpclient, HttpPost request, boolean expectResponse) throws Exception {
		String responseContent = "";
		currentAttempt = 0;
		try {
			for (currentAttempt = 0; currentAttempt <= maxRetriesInCaseOfErrors; currentAttempt++) {
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
	            CloseableHttpResponse httpResponse = null;
	            try {
	            	if (LOG.isDebugEnabled()) {
	            		LOG.debug("Execute request: " + request);
	            	}
	            	httpResponse = httpclient.execute(request);
	            	statusCode = httpResponse.getStatusLine().getStatusCode();
	            	statusMessage = httpResponse.getStatusLine().getReasonPhrase();
	            	if (expectResponse || (statusCode != 204 && statusCode != 205)) {
	                	responseContent = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
	                	if (Util.isEmpty(responseContent)) {
	                		throw new Exception("Empty response received.");
	                	}
	            	}
	            	httpResponse.close();
	            	if (cacheClient == false) {
		    	    	httpclient.close();
	            	}
	            	break;
	            } catch (Exception e) {
	            	if (currentAttempt <= maxRetriesInCaseOfErrors) {
	                	// this can happen, we try it again
	                	LOG.warn("POST request: " + request.getURI() + " failed (" + (currentAttempt + 1) + ". attempt). Waiting " + waitMillisAfterError + "ms and retry request.", e);
	                	Thread.sleep(waitMillisAfterError);
	            	} else {
	                	LOG.error("POST request: " + request.getURI() + " failed.", e);
	                	throw new Exception("POST request: " + request.getURI() + " failed.", e);
	            	}
	            } finally {
	            	if (httpResponse != null) {
	                    httpResponse.close();
	            	}
	            }
			}
		} finally {
        	if (cacheClient == false) {
    	    	httpclient.close();
        	}
		}
        return responseContent;
	}

	public String post(String urlStr, String user, String password, JsonNode node, boolean expectResponse) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("POST " + urlStr + " body: " + node.toString());
		}
        CloseableHttpClient httpclient = createCloseableClient(urlStr, user, password); 
        HttpPost request = new HttpPost(urlStr);
        request.getConfig();
        if (node != null) {
            request.setEntity(buildEntity(node));
            request.addHeader("Accept", "application/json");
            request.addHeader("Content-Type", "application/json;charset=UTF-8");
        }
        return execute(httpclient, request, expectResponse);
	}

	private CloseableHttpClient createCloseableClient(String urlStr, String user, String password) throws Exception {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        if (cachedHttpClient == null) {
            if (user != null && user.trim().isEmpty() == false) {
        		URL url = new URL(urlStr);
                credsProvider.setCredentials(
                        new AuthScope(url.getHost(), url.getPort()),
                        new UsernamePasswordCredentials(user, password));
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(connectionTimeout)
                        .setConnectTimeout(connectionTimeout)
                        .setConnectionRequestTimeout(connectionTimeout)
                        .setRedirectsEnabled(true)
                        .setRelativeRedirectsAllowed(true)
                        .build();
                CloseableHttpClient client = HttpClients.custom()
                        .setDefaultCredentialsProvider(credsProvider)
                        .setDefaultRequestConfig(requestConfig)
                        .build();
                if (cacheClient) {
                	cachedHttpClient = client;
                }
                return client;
            } else {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(connectionTimeout)
                        .setConnectTimeout(connectionTimeout)
                        .setConnectionRequestTimeout(connectionTimeout)
                        .setRedirectsEnabled(true)
                        .setRelativeRedirectsAllowed(true)
                        .build();
                CloseableHttpClient client = HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .build();
                if (cacheClient) {
                	cachedHttpClient = client;
                }
                return client;
            }
        } else {
        	return cachedHttpClient;
        }
	}

	public int getTimeout() {
		return connectionTimeout;
	}

	public void setTimeout(Integer timeout) {
		if (timeout != null) {
			this.connectionTimeout = timeout;
		}
	}

	public void setTimeoutInSec(Integer timeout) {
		if (timeout != null) {
			this.connectionTimeout = (timeout * 1000);
		}
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(Integer connectionTimeout) {
		if (connectionTimeout != null) {
			this.connectionTimeout = connectionTimeout;
		}
	}

	public int getMaxRetriesInCaseOfErrors() {
		return maxRetriesInCaseOfErrors;
	}

	public void setMaxRetriesInCaseOfErrors(Integer maxRetriesInCaseOfErrors) {
		if (maxRetriesInCaseOfErrors != null) {
			this.maxRetriesInCaseOfErrors = maxRetriesInCaseOfErrors;
		}
	}

	public int getCurrentAttempt() {
		return currentAttempt;
	}

	public long getWaitMillisAfterError() {
		return waitMillisAfterError;
	}

	public void setWaitMillisAfterError(Long waitMillisAfterError) {
		if (waitMillisAfterError != null) {
			this.waitMillisAfterError = waitMillisAfterError;
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

	public boolean isCacheClient() {
		return cacheClient;
	}

	public void setCacheClient(boolean cacheClient) {
		this.cacheClient = cacheClient;
	}
	
	public void close() {
		if (cachedHttpClient != null) {
			try {
				cachedHttpClient.close();
			} catch (IOException e) {
				// ignore
			}
		}
	}

}
