package de.jlo.talendcomp.camunda.externaltask;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CamundaClientTest {
	
	private Map<String, Object> globalMap = new HashMap<String, Object>();

	@Before
	public void init() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.DEBUG);
	}
	
	@Test
	public void testFetchAndLock() throws Exception {
		FetchAndLock cc = new FetchAndLock();
		globalMap.put("comp1", cc);
		cc.setCamundaServiceURL("http://camundatest02.gvl.local:10080");
		cc.setCamundaUser("talend_prod");
		cc.setCamundaPassword("8CR!Sy%AT?TVYE476XeB");
		cc.setWorkerId("camundaClientTest_" + System.currentTimeMillis());
		cc.setNumberTaskToFetch(1);
		cc.setMaxRetriesInCaseOfErrors(4);
		cc.setTopicName("calculateConflict");
		cc.addVariable("calculateConflict");
		cc.setLockDuration(5000);
		cc.setTimeBetweenFetches(1000);
		cc.setWaitMillisAfterError(10000);
		cc.setStopTime(60);
		while (cc.nextTask()) {
			System.out.println(cc.getCurrentTaskId());
			System.out.println(cc.getCurrentTaskVariableValueAsObject("calculateConflict", false, false));
		}
		Assert.assertTrue(true);
	}

//	@Test
//	public void testConnect() throws Exception {
//		FetchAndLock cc = new FetchAndLock();
//		globalMap.put("comp1", cc);
//		cc.setCamundaServiceURL("http://camundacdh01.gvl.local:8080");
//		cc.setCamundaUser("talend_prod");
//		cc.setCamundaPassword("8CR!Sy%AT?TVYE476XeB");
//		cc.setWorkerId("camundaClientTest_" + System.currentTimeMillis());
//		cc.setNumberTaskToFetch(1);
//		cc.setMaxRetriesInCaseOfErrors(4);
//		cc.setTopicName("calculateConflict");
//		cc.addVariable("calculateConflict");
//		cc.setLockDuration(5000);
//		cc.setTimeBetweenFetches(1000);
//		cc.setWaitMillisAfterError(10000);
//		cc.setStopTime(60);
//		while (cc.nextTask()) {
//			System.out.println(cc.getCurrentTaskId());
//			System.out.println(cc.getCurrentTaskVariableValueAsObject("calculateConflict", false, false));
//		}
//		Assert.assertTrue(true);
//	}

}
