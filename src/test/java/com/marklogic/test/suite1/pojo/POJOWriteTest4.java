package com.marklogic.test.suite1.pojo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JAXBHandle;
import com.marklogic.test.suite1.AbstractApiTest;
import com.marklogic.test.suite1.GeneralUtils;
import com.marklogic.test.suite1.pojo.Employee;

// illustrates how to write POJOs to the database
@Configuration
@PropertySource(value = { "classpath:contentpump.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class POJOWriteTest4 extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${pojoPath}")
	private String POJO_PATH;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;

	private String COLLECTION_NAME = "";
	String DB_NAME = "";

	@Test
	public void doPOJOWriteTest() throws JAXBException, FileNotFoundException {

		String methodName = new POJOWriteTest4() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		File fl = new File(POJO_PATH);
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);

		COLLECTION_NAME = java.util.UUID.randomUUID().toString();
		// create the client
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		ArrayList<File> lstFiles = GeneralUtils.listFilesForFolder(fl, false, ".*\\.xml");
		// create the document manager
		XMLDocumentManager docMgr = client.newXMLDocumentManager();
		// initialize JAXB for processing the POJO class
		JAXBContext context = JAXBContext.newInstance(Employee.class);
		// create a handle on the POJOs for writing to the database
		JAXBHandle<Employee> writeHandle = new JAXBHandle<Employee>(context);
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
		metadata.getCollections().addAll(COLLECTION_NAME);
		// iterate over the serialized POJOs
		Unmarshaller u = context.createUnmarshaller();
		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			// an identifier for the POJO in the database
			String docId = "/" + COLLECTION_NAME + "_" + lstFiles.get(i).getName();
			// deserialize the POJO
			Employee employee = (Employee) u.unmarshal(docStream);
			// provide a handle for the POJO
			writeHandle.set(employee);

			// write the POJO to the database
			docMgr.write(docId, writeHandle);
		}

		// Marshaller m = context.createMarshaller(context);
		// release the client
		client.release();
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}
