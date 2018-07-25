function addMetaData(content, context)
{
	 var propVal = (context.transform_param == undefined)
	 ? "UNDEFINED" : context.transform_param;
	 var docType = xdmp.nodeKind(content.value);
	 var currentURI = String(content.uri);
	 
	 if (docType == 'document' &&
	 content.value.documentFormat == 'JSON') {
	 // Convert input to mutable object and add new property
	 var newDoc = content.value.toObject();
	 newDoc.userId = propVal;
	 newDoc.timeStamp = fn.formatDateTime(fn.currentDateTime(),"[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01]:[f001][Z]","en","AD","US");
	 // Convert result back into a document
	 content.value = xdmp.unquote(xdmp.quote(newDoc));
 }
 	return content;
};

exports.addMetaData = addMetaData;