(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [
            {
                id: "id",
                dataType: tableau.dataTypeEnum.string
            }, 
            {
                id: "description",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "status",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "amountUSD",
                alias: "amountUSD",
                dataType: tableau.dataTypeEnum.float
            },
            {
                id: "originalAmount",
                alias: "originalAmount",
                dataType: tableau.dataTypeEnum.float
            },
            {
                id: "originalCurrency",
                alias: "originalCurrency",
                dataType: tableau.dataTypeEnum.string
            }, 
            {
                id: "exchangeRate",
                alias: "exchangeRate",
                dataType: tableau.dataTypeEnum.float
            },
            {
                id: "budgetYear",
                alias: "budgetYear",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "flowType",
                alias: "flowType",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "contributionType",
                alias: "contributionType",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "boundary",
                alias: "boundary",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "onBoundary",
                alias: "onBoundary",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "refCode",
                alias: "refCode",
                dataType: tableau.dataTypeEnum.string
            }];

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
            var feat = resp.data.flows,
                tableData = [];

            // Iterate over the JSON object
            for (var i = 0, len = feat.length; i < len; i++) {
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
                    "refCode": feat[i].refCode
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
