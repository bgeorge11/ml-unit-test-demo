package com.marklogic.test.suite1;

import org.junit.Before;

import com.marklogic.mgmt.api.API;
import com.marklogic.mgmt.api.Resource;

public abstract class AbstractApiTest extends AbstractMgmtTest {

	protected API api;

	@Before
	public void setup() {
		api = new API(manageClient);
	}

	protected void deleteIfExists(Resource... resources) {
		for (Resource r : resources) {
			if (r != null && r.exists()) {
				r.delete();
			}
		}
	}
}
