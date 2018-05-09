package com.marklogic.test.suite1.semantics;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.test.suite1.AbstractApiTest;
import com.marklogic.test.suite1.GeneralUtils;
import com.marklogic.test.suite1.springdata.SpringDataOperationsTest4;

@Configuration
@PropertySource(value = { "classpath:suite1.properties", "classpath:user.properties" }, ignoreResourceNotFound = true)
public class SemanticsDataOperationsTest3 extends AbstractApiTest {
	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${triplesPath}")
	private String TRIPLES_PATH;
	@Value("${ontologyPath}")
	private String ONTOLOGY_PATH;
	@Value("${triplesDocCollectionsSuffix}")
	private String TRIPLES_COLLECTION_SUFFIX;
	@Value("${ontologyDocCollectionsSuffix}")
	private String ONTOLOGY_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;

	private String COLLECTION_NAME = "";
	String DB_NAME = "";

	public StringHandle testReadGraph(GraphManager gmgr, String graphURI, String mimeType) {
		
		return (gmgr.read(graphURI, new StringHandle().withMimetype(mimeType)));
	}

	@Test
	public void doOntologyInsert() {

		String methodName = new SemanticsDataOperationsTest3() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		File fl = new File(ONTOLOGY_PATH);
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		ArrayList<File> lstFiles = GeneralUtils.listFilesForFolder(fl, false, ".*\\.ttl");

		GraphManager gmgr = client.newGraphManager();
		gmgr.setDefaultMimetype(RDFMimeTypes.TURTLE);
		COLLECTION_NAME = java.util.UUID.randomUUID().toString() + ONTOLOGY_COLLECTION_SUFFIX;
		for (int i = 0; i < lstFiles.size(); i++) {
			FileHandle tripleHandle = new FileHandle(lstFiles.get(i));
			gmgr.merge(COLLECTION_NAME, tripleHandle);
		}

		StringHandle triples = testReadGraph(gmgr, COLLECTION_NAME, RDFMimeTypes.TURTLE);
		assertNotNull(triples);

		SPARQLQueryManager sqmgr = client.newSPARQLQueryManager();
		SPARQLQueryDefinition query = sqmgr
				.newQueryDefinition("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
						+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
						+ "PREFIX owl:  <http://www.w3.org/2002/07/owl#>"
						+ "PREFIX br:   <http://buildsys.org/ontologies/BrickTag#>" 
						+ "select ?subject " + " WHERE { "
						+ "?subject  rdfs:subClassOf   br:MeasurementProperty_Modifier" + "}");
		JsonNode results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		JsonNode matches = results.path("results").path("bindings");
		assertEquals(165, matches.size()); // 165 is the hard coded result.
		for (int i = 0; i < matches.size(); i++) {
			String subject = matches.get(i).path("subject").path("value").asText();
			// Demo just to get all the subjects. no assertion done with this.
		}
		client.release();
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
	
	@Test
	public void doTriplesInsert() {

		String methodName = new SemanticsDataOperationsTest3() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		File fl = new File(TRIPLES_PATH);
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		ArrayList<File> lstFiles = GeneralUtils.listFilesForFolder(fl, false, ".*\\.ttl");

		GraphManager gmgr = client.newGraphManager();
		gmgr.setDefaultMimetype(RDFMimeTypes.TURTLE);
		COLLECTION_NAME = java.util.UUID.randomUUID().toString() + TRIPLES_COLLECTION_SUFFIX;
		for (int i = 0; i < lstFiles.size(); i++) {
			FileHandle tripleHandle = new FileHandle(lstFiles.get(i));
			gmgr.merge(COLLECTION_NAME, tripleHandle);
		}

		StringHandle triples = testReadGraph(gmgr, COLLECTION_NAME, RDFMimeTypes.TURTLE);
		assertNotNull(triples);

		client.release();
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}
