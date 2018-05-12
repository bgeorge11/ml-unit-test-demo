package com.marklogic.test.suite1.semantics;

import java.util.Date;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.test.suite1.AbstractApiTest;
import com.marklogic.test.suite1.GeneralUtils;

@Configuration
@PropertySource(value = { "classpath:suite1.properties", "classpath:user.properties" }, 
                           ignoreResourceNotFound = true)
public class SemanticsDataOperationsTest4 extends AbstractApiTest {
	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;

	String DB_NAME = "";

	public StringHandle testReadGraph(GraphManager gmgr, String graphURI, String mimeType) {

		return (gmgr.read(graphURI, new StringHandle().withMimetype(mimeType)));
	}

	@Test
	public void doGraphDataOperations() {

		String methodName = new SemanticsDataOperationsTest4() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		GraphManager gmgr = client.newGraphManager();
		gmgr.setDefaultMimetype(RDFMimeTypes.TURTLE);
		SPARQLQueryManager sqmgr = client.newSPARQLQueryManager();
		SPARQLQueryDefinition query = sqmgr.newQueryDefinition(
										"DROP SILENT GRAPH <http://marklogic.com/music> ;"
										);
		sqmgr.executeUpdate(query);

		/*
		 * The sequence done is as below 1. Insert graph triples 2. Query
		 * without inferencing 3. Insert ontology triples 4. Query with
		 * inferencing
		 */
		query = sqmgr.newQueryDefinition("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX music: <http://marklogic.com/musicians/>"
				+ "PREFIX country: <http://marklogic.com/musicians/country/>" 
				+ "INSERT DATA" 
				+ "{"
				+ 		"GRAPH <http://marklogic.com/music>" 
				+ 		"{" 
				+ 			"music:David_Bowie rdf:type \"Singer\" ;"
				+ 			"country:country \"England\" ." 
				+ 			"music:Eric_Clapton rdf:type \"Guitarist\" ;"
				+ 			"country:country \"England\"  ." 
				+ 			"music:Beethovan rdf:type \"Composer\" ;"
				+ 			"country:country \"Germany\" ." 
				+ 			"music:Rahman rdf:type \"Composer\" ;"
				+ 			"country:country \"India\" ." 
				+ 			"music:Rahman rdf:type \"Singer\" ;"
				+ 			"country:country \"India\" ." 
				+ 			"music:Johnson rdf:type \"Violinist\" ;"
				+ 			"country:country \"India\" ." 
				+ 		"}" 
				+ "}"
				);
		sqmgr.executeUpdate(query);

		query = sqmgr.newQueryDefinition("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from " + "	<http://marklogic.com/music>" + "  where {?s rdf:type \"Singer\"}");
		JsonNode results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		JsonNode matches = results.path("results").path("bindings");
		assertEquals(2, matches.size()); // 2 is the hard coded result and no
											// inferencing.

		query = sqmgr.newQueryDefinition("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX music: <http://marklogic.com/musicians/>"
				+ "PREFIX country: <http://marklogic.com/musicians/country/>" 
				+ "PREFIX owl:  <http://www.w3.org/2002/07/owl#>"
				+ "INSERT DATA " 
				+ "{"
				+ "  GRAPH <http://marklogic.com/music>" 
				+ " 	{"
				+ "    		\"Singer\" rdfs:subClassOf \"Musician\" ."
				+ "			\"Composer\" rdfs:subClassOf \"Musician\" ."
				+ "			\"Composer\" owl:sameAs \"Writer\" ."
				+ "    		\"Violinist\" rdfs:subClassOf \"Instrumentalist\" ."
				+ "    		\"Guitarist\" rdfs:subClassOf \"Instrumentalist\" ."
				+ "    		\"Musician\" rdfs:subClassOf \"Artist\" ."
				+ "    		\"Instrumentalist\" rdfs:subClassOf \"Musician\" ."
				+ "    		\"Artist\" rdfs:subClassOf \"Skilled\" ."
				+ "    		\"Skilled\" rdfs:subClassOf \"People\" ." + " 	}" 
				+ "}"
				);
		sqmgr.executeUpdate(query);
		/*
		 * Find all subjects who are 'Skilled'. This should return all Singers,
		 * Composers, Violinists Musicians and Guitarists First query without
		 * any ruleset, then using rulesets RDFS and OWL-HORST
		 */
		query = sqmgr.newQueryDefinition(
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from "
				+ "	<http://marklogic.com/music>" 
				+ "  where {?s rdf:type \"Skilled\"}");
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		matches = results.path("results").path("bindings");
		assertEquals(0, matches.size()); // Ruleset not set, hence no inference
											// results expected.

		query.setRulesets(SPARQLRuleset.RDFS);
		Date inferenceStart = new Date();
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		Date inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(5, matches.size()); // 5 is the expected result with
											// ruleset.

		/*
		 * Try another out-of-box rule set OWL HORST
		 */
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.OWL_HORST);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for OWL HORST inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(5, matches.size()); // 5 is the hard coded result

		/*
		 * Now, the difference made with owl specific ontology. 
		 * Composer has a sameAs relationship with Writer. 
		 * First we will use RDFS rule to return all 'Writers'. The expected result is zero
		 * Then we will use OWL_HORST rule to return all Writers. The expected result is two
		 */
		
		query = sqmgr.newQueryDefinition(
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from " 
				+ "	<http://marklogic.com/music>" 
				+ "  where {?s rdf:type \"Writer\"}"
				);
		query.setRulesets(SPARQLRuleset.RDFS);
		inferenceStart = new Date();
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS inferencing (sameAs) is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(0, matches.size()); // 0 is the expected result with ruleset.
		
		query = sqmgr.newQueryDefinition(
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from " 
				+ "	<http://marklogic.com/music>" 
				+ "  where {?s rdf:type \"Writer\"}");
		query.setRulesets(SPARQLRuleset.OWL_HORST);
		inferenceStart = new Date();
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for OWL HORST (sameAs) inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(2, matches.size()); // 2 is the expected result with ruleset.
		
		query.setRulesets(SPARQLRuleset.OWL_HORST_FULL);
		inferenceStart = new Date();
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for OWL HORST FULL (sameAs) inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(2, matches.size()); // 2 is the expected result with ruleset.
		
		
		client.release();
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + methodName + " is " + 
						(end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}
