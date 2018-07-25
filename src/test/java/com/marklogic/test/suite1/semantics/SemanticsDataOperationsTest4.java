package com.marklogic.test.suite1.semantics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
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
	@Value("${RuleSetPath}")
	private String RULE_SET_PATH;
	String DB_NAME = "";

	public StringHandle testReadGraph(GraphManager gmgr, String graphURI, String mimeType) {

		return (gmgr.read(graphURI, new StringHandle().withMimetype(mimeType)));
	}
	
	private void installRuleSetUsingServerSideCode()
			throws IOException {

		GeneralUtils testUtils = new GeneralUtils();
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Schemas",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		
		ServerEvaluationCall theCall = client.newServerEval();
		String query = 
				" declareUpdate(); "
				+ " var textNode = new NodeBuilder(); "
				+ " textNode.addText( "
				+ "`"
				+ "import \"subClassOf.rules\""
				+ " PREFIX owl: <http://www.w3.org/2002/07/owl#>"
				+ " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ " PREFIX onto: <http://marklogic/onto#>"
				+ " rule \"scm-uni1\" CONSTRUCT {"
				+ " ?a  onto:_unionOf ?m . "
				+ " }"
				+ " {"
				+ " ?a  owl:unionOf ?m ." 
				+ "}"
				+ " rule \"scm-uni\"     CONSTRUCT"  
				+ " {"
				+ " ?c rdfs:subClassOf ?a ."
				+ " ?a  onto:_unionOf    ?d"
				+ " }"
				+ " {"
				+ "  ?a  onto:_unionOf    ?m . " 
				+ "  ?m  rdf:first        ?c . "     
				+ "  ?m  rdf:rest         ?d . "
				+ " } "
				+ " ` "
				+ " ); "
				+ "xdmp.documentInsert('/rules/unionOf.rules',textNode.toNode());"
				;
		theCall.javascript(query);
		/*
		 * TODO the above is not the right way of calling external module.
		 */
		String response = theCall.evalAs(String.class);

		testUtils.logComments(new Date().toString() + " Installed Ruleset for UnionOf", LOGLEVEL);
		client.release();

	}
	
	private void deleteRuleSet() throws FileNotFoundException
	{
		
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Schemas",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		
		File fl = new File(RULE_SET_PATH);

		GeneralUtils genUtils = new GeneralUtils();

		ArrayList<File> lstFiles = genUtils.listFilesForFolder(fl, false, ".*\\.rules");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for Text documents
			TextDocumentManager docMgr = client.newTextDocumentManager();

			// create a handle on the content
			InputStreamHandle handle = new InputStreamHandle(docStream);
			// add a collection tag
			// write the document content
			docMgr.delete("/rules/"+ fileName);
	}
		}
	
	private void installRuleSetFromFileSystem() throws FileNotFoundException
	{
		
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Schemas",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		
		File fl = new File(RULE_SET_PATH);

		GeneralUtils genUtils = new GeneralUtils();

		ArrayList<File> lstFiles = genUtils.listFilesForFolder(fl, false, ".*\\.rules");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for Text documents
			TextDocumentManager docMgr = client.newTextDocumentManager();

			// create a handle on the content
			InputStreamHandle handle = new InputStreamHandle(docStream);
			docMgr.write("/rules/"+ fileName,handle);
	}
		}
	

	@Test
	public void doGraphDataOperations() throws IOException {

		String methodName = new SemanticsDataOperationsTest4() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);
		
		String triplesQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
			    + "PREFIX music: <http://marklogic.com/musicians#>"
			    + "PREFIX rel: <http://marklogic.com/rel#>"
			    + "PREFIX typ: <http://marklogic.com/typ#>"
			    + "INSERT DATA {"
				  + "GRAPH <http://marklogic.com/music>   {"
				    + "music:Artist1 rdf:type rdf:Singer  ."
				    + "music:Artist2 rdf:type rdf:Guitar . "
				    + "music:Artist2 typ:type \"instrument\" ."
				    + "music:Artist3 rdf:type rdf:Composer ." 
				    + "music:Artist3 typ:type \"music\"."
				    + "music:Artist4 rdf:type rdf:Composer ."
				    + "music:Artist4 typ:type \"music\"."
				    + "music:Artist4 rdf:type rdf:Singer ."
				    + "music:Artist5 rdf:type rdf:Violin ."
				    + "music:Artist6 rdf:type rdf:Violin ."
				    + "music:Artist6 rdf:type rdf:Guitar . "
				    + "music:Artist7 rdf:type rdf:Guitar ."
				    + "music:Artist8 rdf:type rdf:Sitar ."
				    + "music:Artist9 rdf:type rdf:Sitar ."
				    + "music:Artist10 rdf:type rdf:Fiddle ."
				    + "music:Artist11 rdf:type rdf:Fiddle ."
				    + "music:Artist12 rdf:type rdf:Composer  ."
				    + "music:Artist13 rdf:type rdf:Piano ."
				    + "music:Artist4 rel:friendOf music:Artist6 ."
				    + "music:Artist4 rel:teacherOf music:Artist4 ."
				    + "music:Artist4 rel:studentOf music:Artist12 ." 
				    + "music:Artist12 rel:studentOf music:Artist12 ." 
				    + "} }";
	    String ontologyQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
						+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
						+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>"
						+ "PREFIX rel: <http://marklogic.com/rel#>"
						+ "PREFIX typ: <http://marklogic.com/typ#>"
						    
						+ "INSERT DATA"
						+ "{"
						+ 		" GRAPH <http://marklogic.com/music>"
						+ 		"{"
						+ 			" rdf:Singer rdfs:subClassOf" 
						+ 			" rdf:Artist ."
						+ 			" rdf:Composer rdfs:subClassOf" 
						+ 			" rdf:Artist ."
						+ 			" rdf:Violin rdfs:subClassOf" 
						+ 			" rdf:Artist ."
						+ 			" rdf:Guitar rdfs:subClassOf" 
						+ 			" rdf:Artist ."
						+ 			" rdf:Musician  owl:equivalentClass" 
						+ 			" rdf:Artist  . "
						+ 			" rdf:Musician  a owl:Restriction;" 
						+ 				" owl:onProperty typ:type;" 
						+ 				" owl:hasValue \"music\" . "
						+ 			" rel:teacherOf a owl:irreflexiveProperty ."
						+ 			" rel:studentOf a owl:ObjectProperty; "
						+ 							" a owl:irreflexiveProperty ." 
						+ 			" rdf:Instruments owl:unionOf" 
						+ 							"("
						+ 								" rdf:Violin"
						+ 								" rdf:Guitar" 
						+ 								" rdf:Sitar"
						+ 								" rdf:Fiddle"
						+ 								" rdf:Piano"
						+ 							")"
						+ 			"}"
						+ "}";

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
		query = sqmgr.newQueryDefinition(triplesQuery);
		sqmgr.executeUpdate(query);

		query = sqmgr.newQueryDefinition("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from " + "	<http://marklogic.com/music>" + "  where {?s rdf:type rdf:Singer}");
		JsonNode results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		JsonNode matches = results.path("results").path("bindings");
		assertEquals(2, matches.size()); // 2 is the hard coded result and no
											// inferencing.

		query = sqmgr.newQueryDefinition(ontologyQuery);
		sqmgr.executeUpdate(query);
		/*
		 * Find all subjects who are 'Artist'. This should return all Singers,
		 * Composers, Violinists Musicians and Guitarists First query without
		 * any ruleset, then using rulesets RDFS and OWL-HORST
		 */
		query = sqmgr.newQueryDefinition(
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from "
				+ "	<http://marklogic.com/music>" 
				+ "  where {?s rdf:type rdf:Artist}");
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		matches = results.path("results").path("bindings");
		assertEquals(0, matches.size()); // Ruleset not set, hence no inference
											// results expected.

		Date inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.SUBCLASS_OF);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		Date inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for SUB CLASS OF inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the hard coded result
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.RDFS);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the expected result with
											// ruleset.
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.RDFS_FULL);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS_FULL inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the expected result with
											// ruleset.
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.RDFS_PLUS);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS_PLUS inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the expected result with
											// ruleset.
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for RDFS_PLUS_FULL inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the hard coded result
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.OWL_HORST);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for OWL HORST inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the hard coded result
		
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.OWL_HORST_FULL);
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for OWL_HORST_FULL inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(8, matches.size()); // 8 is the hard coded result
		
		//installRuleSetUsingServerSideCode();
		installRuleSetFromFileSystem();
		query = sqmgr.newQueryDefinition(
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "	SELECT * from "
				+ "	<http://marklogic.com/music>" 
				+ "  where {?s rdf:type rdf:Instruments}");
		inferenceStart = new Date();
		query.setRulesets(SPARQLRuleset.ruleset("/rules/unionOf.rules"));
		
		results = sqmgr.executeSelect(query, new JacksonHandle()).get();
		inferenceEnd = new Date();
		genTestUtils.logComments(
				"Execution time for UNION OF inferencing is  " + 
						(inferenceEnd.getTime() - inferenceStart.getTime()) + " milli seconds.",
				LOGLEVEL);
		matches = results.path("results").path("bindings");
		assertEquals(9, matches.size()); // 9 is the hard coded result
		deleteRuleSet();
		client.release();
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + methodName + " is " + 
						(end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}
