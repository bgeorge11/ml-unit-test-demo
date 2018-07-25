package com.marklogic.test.suite1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.admin.QueryOptionsManager;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.expression.PlanBuilder.ExportablePlan;
import com.marklogic.client.expression.PlanBuilder.ModifyPlan;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.ValuesHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.ValuesDefinition;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.type.CtsQueryExpr;
import com.marklogic.client.type.CtsReferenceExpr;
import com.marklogic.client.type.PlanAggregateCol;
import com.marklogic.client.type.XsIntSeqVal;
import com.marklogic.client.type.XsStringSeqVal;
import com.marklogic.client.type.XsStringVal;
import com.marklogic.mgmt.admin.AdminManager;
import com.marklogic.mgmt.api.API;
import com.marklogic.mgmt.api.database.Database;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class RowManagerOperationsTest5 extends AbstractApiTest {
	
	private Database db;

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${logLevel}")
	private String LOGLEVEL;
	@Value("${mlRangeElementIndexLocalName}")
	private String RANGE_ELEMENT_INDEX_LOCAL_NAME;
	@Value("${mlRangeElementIndexScalarType}")
	private String RANGE_ELEMENT_INDEX_SCALAR_TYPE;
	@Value("${mlRangeElementIndexNS}")
	private String RANGE_ELEMENT_INDEX_NS;
	@Value("${JSONDocPath}")
	private String JSON_DOC_PATH;
	@Value("${JSONDocCollectionSuffix}")
	private String JSON_DOC_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	private Long TOTAL_JSON_DOCS_ADDED = 0L;
	private String DB_NAME = "";

	@After
	public void teardown() {

	}

	public void loadJSONDocuments(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		File fl = new File(JSON_DOC_PATH);

		GeneralUtils genUtils = new GeneralUtils();

		ArrayList<File> lstFiles = genUtils.listFilesForFolder(fl, false, ".*\\.json");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for JSON documents
			JSONDocumentManager docMgr = client.newJSONDocumentManager();

			// create a handle on the content
			InputStreamHandle handle = new InputStreamHandle(docStream);
			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			// add a collection tag
			metadata.getCollections().addAll(COLLECTION_NAME);
			// write the document content
			docMgr.write("/" + COLLECTION_NAME + "_" + fileName, metadata, handle);
			TOTAL_JSON_DOCS_ADDED++;

		}

	}
			
	protected CtsQueryExpr[] getAcctSkQuery(Integer[] accountArr, PlanBuilder planBuilder ){
		CtsQueryExpr[] acctQueryArray = new CtsQueryExpr[accountArr.length];
		for(int i=0; i<accountArr.length; i++){
			XsStringSeqVal propertyName = planBuilder.xs.string("position/acctSk");
            XsIntSeqVal value = planBuilder.xs.intSeq(Integer.valueOf(accountArr[i]));
            XsStringVal operator = planBuilder.xs.string("=");
            
			CtsQueryExpr query = planBuilder.cts.pathRangeQuery(propertyName, operator, value);
			acctQueryArray[i] = query; 
		}
	
		return acctQueryArray;
	}

	protected PlanAggregateCol[] getAggrColumns(PlanBuilder planBuilder, String[] aggrFields){
		PlanAggregateCol[] aggrColArry = new PlanAggregateCol[aggrFields.length];
		for(int i=0; i<aggrFields.length; i++){
			aggrColArry[i] =  planBuilder.sum(aggrFields[i], aggrFields[i]);
		}
		return aggrColArry;
	}
	
	@Test
	public void testLoadAndQueryWithOptions() throws Exception {

		String methodName = new RowManagerOperationsTest5() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		String prefix = java.util.UUID.randomUUID().toString();
		String COLLECTION_NAME = prefix + JSON_DOC_COLLECTION_SUFFIX;
		Integer[] accountArr = new Integer[3];
		String[] aggrFields = new String[3];
		Date start = new Date();
		String calendarDate = "2013-07-24";
		
		GeneralUtils genTestUtils = new GeneralUtils();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);
		
		accountArr[0] = 9994;
		accountArr[1] = 9158;
		accountArr[2] = 3245;
		aggrFields[0] = "mvLclAmt";
		aggrFields[1] = "exchRtAmt";
		aggrFields[2] = "adjBaseLclAmt";
		
		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		
		/* Create the below path range indexes
		position/acctSk
	    position/mvLclAmt
	    position/exchRtAmt
	    position/adjBaseLclAmt
	    position/valnAsOfDate
	    position/astIssType
	    position/instrId"
    */
			
//		DatabaseClient restClient = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
//				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
//
//		ServerEvaluationCall theCall = restClient.newServerEval();
//		String query = 
//				"xquery version \"1.0-ml\";"
//			+ " import module namespace admin=\"http://marklogic.com/xdmp/admin\""
//			+ " 				at \"/MarkLogic/admin.xqy\";"
//			+ " declare variable $dbid := xdmp:database(\""+DB_NAME+"\");"
//			+ " declare variable $config := admin:get-configuration();"
//			+ " declare variable $pathspec:=  admin:database-range-path-index("
//			+ " $dbid,"
//			+ " \"string\","
//			+ " \"position/astIssType\","
//			+ " \"http://marklogic.com/collation/\","
//			+ " fn:false(),"
//			+ " \"ignore\");"
//			+ " admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pathspec));";
//		theCall.xquery(query);
//		System.out.println(query);
//		String response = theCall.evalAs(String.class);
		

		//Load some and count documents
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
			
		RowManager rowMgr = client.newRowManager();
		PlanBuilder planBuilder = rowMgr.newPlanBuilder();
		CtsQueryExpr[] acctQueryArray= getAcctSkQuery(accountArr, planBuilder);
		
	    Map<String, CtsReferenceExpr> indexes = new HashMap<String, CtsReferenceExpr>();
	    indexes.put("acctSk", planBuilder.cts.pathReference("position/acctSk"));
	    indexes.put("mvLclAmt", planBuilder.cts.pathReference("position/mvLclAmt"));
	    indexes.put("exchRtAmt", planBuilder.cts.pathReference("position/exchRtAmt"));
	    indexes.put("adjBaseLclAmt", planBuilder.cts.pathReference("position/adjBaseLclAmt"));
	    
	    Map<String, CtsReferenceExpr> indexes1 = new HashMap<String, CtsReferenceExpr>();
	    indexes1.put("acctSk", planBuilder.cts.pathReference("position/acctSk"));
	    indexes1.put("valnAsOfDate", planBuilder.cts.pathReference("position/valnAsOfDate"));
	    
	    ModifyPlan  extractPositions1 = planBuilder.fromLexicons(indexes)
	    		.where(planBuilder.cts.collectionQuery("positions"))
				.where(planBuilder.cts.orQuery(acctQueryArray))
				.where(planBuilder.cts.pathRangeQuery("position/calendarDate", "=", calendarDate))
				.groupBy(planBuilder.colSeq(planBuilder.col("acctSk")),
						planBuilder.aggregateSeq(getAggrColumns(planBuilder, aggrFields)));

	    		
	    ModifyPlan  extractPositions2 = planBuilder.fromLexicons(indexes1)
	    		.where(planBuilder.cts.collectionQuery("positions"))
				.where(planBuilder.cts.orQuery(acctQueryArray))
				.where(planBuilder.cts.pathRangeQuery("position/calendarDate", "=", calendarDate))
				.select()
				.whereDistinct();		
	    ModifyPlan  extractPositions = extractPositions2.joinInner(extractPositions1,
	    		                                         planBuilder.on("acctSk", "acctSk"));
	    
		JacksonHandle jacksonHandle = new JacksonHandle();
	    jacksonHandle.setMimetype("application/json");

	    rowMgr.resultDoc(extractPositions, jacksonHandle);
	    
	    JsonNode jsonResults = jacksonHandle.get();
	    JsonNode jsonBindingsNodes = jsonResults.path("rows");
	    genTestUtils.logComments("# OF rows returned by Optic innerjoin" + 
	    							jsonBindingsNodes.size(), LOGLEVEL);
	    assertEquals(3,jsonBindingsNodes.size());
	    
	    Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " 
		        + (end.getTime() - start.getTime()) / 1000 
		        + " seconds.", LOGLEVEL);
	}

}