package de.jlo.talendcomp.camunda.externaltask;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CamundaClientTest {
	
	@Before
	public void init() {
		BasicConfigurator.configure();
	}

	@Test
	public void testFetchAndLock() throws Exception {
		CamundaClient cc = new CamundaClient();
		cc.setCamundaServiceEndpointURL("http://camundatest02.gvl.local:10080");
		cc.setCamundaUser("talend_test");
		cc.setCamundaPassword("talend_test");
		cc.setWorkerId("camundaClientTest");
		cc.setNumberTaskToFetch(1);
		cc.setTopicName("calculateConflict");
		cc.addVariable("calculateConflict");
		int fetchedTask = cc.fetchAndLock();
		System.out.println("Fetched task: " + fetchedTask);
		Assert.assertTrue(true);
	}
}
