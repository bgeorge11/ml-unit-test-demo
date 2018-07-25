var envelope = require("/marklogic.rest.transform/envelope/assets/transform.sjs");
function enrich(content, context) {
    var doc = content.value.root.toObject();
    var calendarID = xdmp.urlEncode(doc.CAL_ID);
    var gregorianDt = doc.GREG_DTE
    var dateTimeFormatwithTZ = "[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01].[f000001][Z]";
    var dateTimeFormatwithoutTz = "[Y0001]-[M01]-[D01] [H01]:[m01]:[s01].[f000001]";
    var newLastChangeTs = xdmp.parseDateTime(dateTimeFormatwithoutTz,doc.LAST_CHG_TMS);
    var URILastPart = fn.formatDateTime(newLastChangeTs, "[Y0001]-[M01]-[D01]-[H01]-[m01]");
    var isDelete = 0;
    var params = {
        entityVersion: "1.0",
        ingestSourceSystem: "FT_T_CADP",
        ingestSourceSystemVersion: "1.0"
    };
    
    if (doc.DM_OPERATION_TYPE == 'D')
    {
      isDelete = 1;
    } 
    
    /*
    	If this is a duplicate URI, right away drop the document by generating custom error. 
    */
    var exactMatchedURI = cts.uriMatch("/edw/holidayDate/" + calendarID + 
                                       "/" + gregorianDt + "/" + URILastPart + ".json").toArray(); 
                                       
    if (exactMatchedURI.length > 0)
    {
    	fn.error(xs.QName("ERROR"), "Duplicate Record");
    }
    
                                      
    /*Calculate the effective start date and time
     * 1. Get current Date time
     * 2. Format the current date time
     * 3. Calculate new time which is one micro-second less
     * 4. Format the effective date end time
     */
    
    var currentTime = fn.currentDateTime();
    var effective_start = fn.formatDateTime(currentTime,
                                             dateTimeFormatwithTZ, "en", "AD", "US");
    var aMicroSecondLess = currentTime.subtract(xs.dayTimeDuration("PT0.000001S"));
    var effective_end = fn.formatDateTime(new Date('Dec 31 9999 00:00:00 GMT-0000'), 
                                                    dateTimeFormatwithTZ, "en", "AD", "US");
 
    /* Get the current document. Only one URI expected in latest and calendar collection.
     * However, handle multiple latest documents that might come up.
     */
    var matchedURIs = cts.uriMatch("/edw/holidayDate/" + calendarID + "/" + gregorianDt + "*.json",
        "case-sensitive",
        cts.jsonPropertyValueQuery("S_ROW_EFF_END",
                                            "9999-12-31T00:00:00.000000")).toArray()
                                      
    var arrLength = matchedURIs.length;
    var oldContent;
    var terminateOldDoc = true;
    var ignoreNewDoc = false;
    var oldLastChangeTs ;
    for (var i = 0; i < arrLength; i++) {
        oldContent = cts.doc(matchedURIs[i]);
        let obj = oldContent.toObject();
        /* 
        *  If the new document LAST_CHG_TMS is before the matched old document, then do not terminate the 
        *  old document.  This can also be implemented by matching only those URIs which are having LAST_CHG_TMS 
        * before the current document's LAST_CHG_TMS
        */
        oldLastChangeTs = xdmp.parseDateTime(dateTimeFormatwithoutTz,obj.envelope.content.LAST_CHG_TMS);
             
             if (oldLastChangeTs.gt(newLastChangeTs)) {
                   terminateOldDoc = false;
                   ignoreNewDoc = true; //If atleast one document is found later, ignore the new document
                   context.collections = "calendar-exception";
                   effective_end = effective_start; 
                   content.uri = "/edw/holidayDate/" + calendarID + "/" + gregorianDt 
                                  + "/" + URILastPart + ".json"
             }
             else  if (oldLastChangeTs.eq(newLastChangeTs)) {
             		fn.error(xs.QName("ERROR"), "Duplicate Record");
             }
             else {
                   terminateOldDoc = true; 
                   ignoreNewDoc = false; 
             }
       
        /*
        *    Update new effective end date to the matching URIs
        */
        if (terminateOldDoc == true) {
              obj.envelope.audit.S_ROW_EFF_END = fn.formatDateTime(aMicroSecondLess, 
                                                            dateTimeFormatwithTZ, "en", "AD", "US");
              if (isDelete) /* If the new document is delete, remove the old document from latest*/ 
              {
                  xdmp.documentInsert(matchedURIs[i], obj, {collections:["calendar"]});
              }
             else
             {
                  xdmp.documentInsert(matchedURIs[i], obj, {collections:["calendar"]});
             }
             ignoreNewDoc = false;
        }
        
    }
      
      if (ignoreNewDoc == false) 
      {
             if (isDelete)
             {
                   effective_end = effective_start;
                   context.collections = "calendar";
             }
             else 
             {
                   context.collections = ["calendar", "latest"];
             }
             content.uri = "/edw/holidayDate/" + calendarID + "/" + gregorianDt 
                          + "/" + URILastPart + ".json"
      }
      
      var newContent = envelope.env("holidayDate", "referenceData", params, 
                                                   doc, effective_start, effective_end);
      content.value = xdmp.toJSON(newContent);
      return content;
}
exports.transform = enrich;
