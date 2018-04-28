(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        


        var cols = [
            {id: "id",dataType: tableau.dataTypeEnum.string},
            {id: "description",dataType: tableau.dataTypeEnum.string},
            {id: "status",dataType: tableau.dataTypeEnum.string},
            {id: "amountUSD",alias: "amountUSD",dataType: tableau.dataTypeEnum.float},
            {id: "originalAmount",alias: "originalAmount",dataType: tableau.dataTypeEnum.float},
            {id: "originalCurrency",alias: "originalCurrency",dataType: tableau.dataTypeEnum.string},
            {id: "exchangeRate",alias: "exchangeRate",dataType: tableau.dataTypeEnum.float},
            {id: "budgetYear",alias: "budgetYear",dataType: tableau.dataTypeEnum.string},
            {id: "flowType",alias: "flowType",dataType: tableau.dataTypeEnum.string},
            {id: "contributionType",alias: "contributionType",dataType: tableau.dataTypeEnum.string},
            {id: "boundary",alias: "boundary",dataType: tableau.dataTypeEnum.string},
            {id: "onBoundary",alias: "onBoundary",dataType: tableau.dataTypeEnum.string},
            {id: "refCode",alias: "refCode",dataType: tableau.dataTypeEnum.string},
            {id: "planId",alias: "planId",dataType: tableau.dataTypeEnum.string},
            {id: "planName",alias: "planName",dataType: tableau.dataTypeEnum.string},
            {id: "planBehavior",alias: "planBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destOrganisationId",alias: "destOrganisationId",dataType: tableau.dataTypeEnum.string},
            {id: "destOrganisationName",alias: "destOrganisationName",dataType: tableau.dataTypeEnum.string},
            {id: "destOrganisationBehavior",alias: "destOrganisationBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destOrganisationType",alias: "destOrganisationType",dataType: tableau.dataTypeEnum.string},
            {id: "destClusterId",alias: "destClusterId",dataType: tableau.dataTypeEnum.string},
            {id: "destClusterName",alias: "destClusterName",dataType: tableau.dataTypeEnum.string},
            {id: "destClusterBehavior",alias: "destClusterBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destGlobalClusterId",alias: "destGlobalClusterId",dataType: tableau.dataTypeEnum.string},
            {id: "destGlobalClusterName",alias: "destGlobalClusterName",dataType: tableau.dataTypeEnum.string},
            {id: "destGlobalClusterBehavior",alias: "destGlobalClusterBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destLocationId",alias: "destLocationId",dataType: tableau.dataTypeEnum.string},
            {id: "destLocationName",alias: "destLocationName",dataType: tableau.dataTypeEnum.string},
            {id: "destLocationBehavior",alias: "destLocationBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destProjectId",alias: "destProjectId",dataType: tableau.dataTypeEnum.string},
            {id: "destProjectName",alias: "destProjectName",dataType: tableau.dataTypeEnum.string},
            {id: "destProjectCode",alias: "destProjectCode",dataType: tableau.dataTypeEnum.string},
            {id: "destProjectBehavior",alias: "destProjectBehavior",dataType: tableau.dataTypeEnum.string},
            {id: "destUsageYearId",alias: "destUsageYearId",dataType: tableau.dataTypeEnum.string},
            {id: "destUsageYearName",alias: "destUsageYearName",dataType: tableau.dataTypeEnum.string},
            {id: "destUsageYearBehavior",alias: "destUsageYearBehavior",dataType: tableau.dataTypeEnum.string}
        ];

            var tableSchema = {
                id: "flowFeed",
                alias: "Flows on FTS",
                columns: cols
            };

        schemaCallback([tableSchema]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        $.getJSON("https://api.hpc.tools/v1/public/fts/flow?planid=638", function(resp) {
            var feat = resp.data.flows,tableData = [];
            var planId = "";
            var planName = "";
            var planBehavior = "";
            var destOrganisationId = "";
            var destOrganisationName = "";
            var destOrganisationBehavior = "";
            var destOrganisationType = "";
            var destClusterId = "";
            var destClusterName = "";
            var destClusterBehavior = "";
            var destGlobalClusterId = "";
            var destGlobalClusterName = "";
            var destGlobalClusterBehavior = "";
            var destLocationId = "";
            var destLocationName = "";
            var destLocationBehavior = "";
            var destProjectId ="";
            var destProjectName ="";
            var destProjectCode ="";
            var destProjectBehavior ="";
            var destUsageYearId ="";
            var destUsageYearName ="";
            var destUsageYearBehavior ="";
            var destObjects = undefined;
    
            
            
            // Iterate over the JSON object
            for (var i = 0, len = feat.length; i < len; i++) {
                destObjects = feat[i].destinationObjects;
                if(destObjects!=undefined){
                    for (v = 0; v < destObjects.length; v++) {
                        switch (destObjects.type) {
                            case "Plan":
                                planId = destObjects.planId;
                                planName = destObjects.planName;
                                planBehavior = destObjects.planBehavior;
                                break;
                            case "Organization":
                                destOrganisationId = destObjects.destOrganisationId;
                                destOrganisationName = destObjects.destOrganisationName;
                                destOrganisationBehavior = destObjects.destOrganisationBehavior;
                                destOrganisationType = destObjects.destOrganisationType;
                                break;
                            case "Cluster":
                                destClusterId = destObjects.destClusterId;
                                destClusterName = destObjects.destClusterName;
                                destClusterBehavior = destObjects.destClusterBehavior;
                                break;
                            case "GlobalCluster":
                                destGlobalClusterId = destObjects.destGlobalClusterId;
                                destGlobalClusterName = destObjects.destGlobalClusterName;
                                destGlobalClusterBehavior = destObjects.destGlobalClusterBehavior;
                                break;
                            case "Location":
                                destLocationId = destObjects.destLocationId;
                                destLocationName = destObjects.destLocationName;
                                destLocationBehavior = destObjects.destLocationBehavior;
                                break;
                            case "Project":
                                destProjectId =destObjects.destProjectId;
                                destProjectName =destObjects.destProjectName;
                                destProjectCode =destObjects.destProjectCode;
                                destProjectBehavior =destObjects.destProjectBehavior;
                                break;
                            case "UsageYear":
                                destUsageYearId =destObjects.destUsageYearId;
                                destUsageYearName =destObjects.destUsageYearName;
                                destUsageYearBehavior =destObjects.destUsageYearBehavior;
                                break;
                        }
                    }
                }
                
                
                tableData.push({
                    "id": feat[i].id,
                    "description": feat[i].description,
                    "status": feat[i].status,
                    "amountUSD": feat[i].amountUSD,
                    "originalAmount": feat[i].originalAmount,
                    "originalCurrency": feat[i].originalCurrency,
                    "exchangeRate": feat[i].exchangeRate,
                    "budgetYear": feat[i].budgetYear,
                    "flowType": feat[i].flowType,
                    "contributionType": feat[i].contributionType,
                    "boundary": feat[i].boundary,
                    "onBoundary": feat[i].onBoundary,
                    "refCode": feat[i].refCode,
                    "planId": planId,
                    "planName": planName,
                    "planBehavior": planBehavior,
                    "destOrganisationId": destOrganisationId,
                    "destOrganisationName": destOrganisationName,
                    "destOrganisationBehavior": destOrganisationBehavior,
                    "destOrganisationType": destOrganisationType,
                    "destClusterId": destClusterId,
                    "destClusterName": destClusterName,
                    "destClusterBehavior": destClusterBehavior,
                    "destGlobalClusterId": destGlobalClusterId,
                    "destGlobalClusterName": destGlobalClusterName,
                    "destGlobalClusterBehavior": destGlobalClusterBehavior,
                    "destLocationId": destLocationId,
                    "destLocationName": destLocationName,
                    "destLocationBehavior": destLocationBehavior,
                    "destProjectId": destProjectId,
                    "destProjectName": destProjectName,
                    "destProjectCode": destProjectCode,
                    "destProjectBehavior": destProjectBehavior,
                    "destUsageYearId": destUsageYearId,
                    "destUsageYearName": destUsageYearName,
                    "destUsageYearBehavior": destUsageYearBehavior
                });
            }

            table.appendRows(tableData);
            doneCallback();
        });
    };

    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "FTS Mali 2018"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
