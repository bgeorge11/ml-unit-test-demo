package com.marklogic.test.suite1;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.marklogic.test.suite1.pojo.POJOOperationsTest5;
import com.marklogic.test.suite1.semantics.SemanticsDataOperationsTest3;
import com.marklogic.test.suite1.semantics.SemanticsDataOperationsTest4;
import com.marklogic.test.suite1.springdata.SpringDataOperationsTest4;

public class StepBRunner {

	@Test
	public void testAllDataOperations() {
		Class[] cls = { MLCPDataOperationsTest1.class, 
						JSONDataOperationsTest2.class, 
						XMLDataOperationsTest3.class,
						BinaryLoadTest2.class, 
						JSONDMSDKOperationsTest4.class,
						CSVDMSDKOperationsTest5.class,
						PartialExtractOperationsTest5.class,
						QueryOptionsOperationsTest5.class,
						POJOOperationsTest5.class, 
						SpringDataOperationsTest4.class,
						SemanticsDataOperationsTest3.class,
						SemanticsDataOperationsTest4.class};

		Result results = JUnitCore.runClasses(ParallelComputer.methods(), cls);

		// System.out.println("-------------DATA OPERATIONS TEST
		// RESULTS---------------");
		// System.out.println("Total RunCount = " + results.getRunCount());
		// System.out.println("Failure Count = " + results.getFailureCount());
		// System.out.println("Run Time = " + results.getRunTime() / 1000 + "
		// seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out.println("Data Operations Failure details " + failure.get(i).getTestHeader() + " "
					+ failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());
	}
}
