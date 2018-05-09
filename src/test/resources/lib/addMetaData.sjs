function addMetaData(content, context)
{
	 var propVal = (context.transform_param == undefined)
	 ? "UNDEFINED" : context.transform_param;
	 var docType = xdmp.nodeKind(content.value);
	 var currentURI = String(content.uri);
	 var arrOutputCollections = context.collections;
	 
	 if (docType == 'document' &&
	 content.value.documentFormat == 'JSON') {
	 // Convert input to mutable object and add new property
	 var newDoc = content.value.toObject();
	 newDoc.userId = propVal;
	 newDoc.timeStamp = fn.formatDateTime(fn.currentDateTime(),"[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01]:[f001][Z]","en","AD","US");
	 // Convert result back into a document
	 content.value = xdmp.unquote(xdmp.quote(newDoc));
	 // A sample code of how to change URI of a document. Does not make any change in the Junit test as the collections are changed dynamically
	 var arrCollections = xdmp.documentGetCollections(currentURI);
	 var arrLength = arrCollections.length;
	 for (var i = 0; i < arrLength; i++) {
    		if (arrOutputCollections.includes(arrCollections[i])) {
    			content.uri = currentURI + "_" + i;
    		}
     xdmp.documentDelete(currentURI);
    			
    }
	
	 
 }
 	return content;
};
module.exports = { 
	addMetaData:addMetaData
	};