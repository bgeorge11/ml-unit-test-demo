const metaDataPlugin = require('/marklogic.rest.transform/addMetaData/assets/transform.sjs');

function mainTransform(content, context)
{
	var contentWithMetaData = metaDataPlugin.addMetaData(content, context);
	
	return contentWithMetaData;
};
exports.transform = mainTransform;