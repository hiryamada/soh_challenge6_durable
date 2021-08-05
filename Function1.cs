// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json.Linq;

namespace sohchallenge6durablefunc
{
    public class BlobInfo
    {
        public string Url { get; }
        public string BatchNo { get; }
        public string EventName { get; }

        public bool Valid { get; }
        public BlobInfo(EventGridEvent eventGridEvent)
        {
            var s = eventGridEvent.Data.ToString();
            var json = JsonConvert.DeserializeObject<Dictionary<string, object>>(s);
            Url = json["url"].ToString();
            var match = Regex.Match(Url, "/(\\d+)-([^/]+)$");
            BatchNo = match.Groups[1].Value ?? "";
            EventName = match.Groups[2].Value ?? "";
            Valid = match.Success;
        }
        public override string ToString()
        {
            return $"BlobInfo Url={Url}, BatchNo={BatchNo}, EventName={EventName}";
        }
    }
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run(
            [EventGridTrigger]EventGridEvent eventGridEvent,
            [DurableClient] IDurableOrchestrationClient client, 
            ILogger log)
        {

            var blobInfo = new BlobInfo(eventGridEvent);
            var batchNo = blobInfo.BatchNo;
            var status = await client.GetStatusAsync(batchNo);

            if (status == null)
            {
                await client.StartNewAsync("Challenge6_Orchestrator", batchNo);
            }
            if (blobInfo.Valid)
            {
                await client.RaiseEventAsync(batchNo, blobInfo.EventName, blobInfo.Url);
            }
        }

        [FunctionName("Challenge6_Orchestrator")]
        public static async Task Orchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {

            // tasks(wait events) for upload
            var upload1 = context.WaitForExternalEvent<string>("OrderHeaderDetails.csv");
            var upload2 = context.WaitForExternalEvent<string>("OrderLineItems.csv");
            var upload3 = context.WaitForExternalEvent<string>("ProductInformation.csv");

            // wait all files to be uploaded
            await Task.WhenAll(upload1, upload2, upload3);

            // combine all uploaded csv files using the API
            var values = new Dictionary<string, string>
            {
                {"orderHeaderDetailsCSVUrl", upload1.Result },
                {"orderLineItemsCSVUrl", upload2.Result },
                {"productInformationCSVUrl", upload3.Result },
            };
            var combined = await context.CallActivityAsync<string>("Challenge6_Combine", values);

            // insert combined data(json) into Cosmos DB "Orders" collection
            await context.CallActivityAsync("Challenge6_Insert", combined);
        }

        [FunctionName("Challenge6_Combine")]
        public static async Task<string> Combine([ActivityTrigger] Dictionary<string, string> values, ILogger log)
        {
            var client = new HttpClient();
            var content = JsonConvert.SerializeObject(values);
            var requestUri = new Uri("https://serverlessohmanagementapi.trafficmanager.net/api/order/combineOrderContent");
            var response = await client.PostAsync(requestUri, new StringContent(content));
            var responseString = await response.Content.ReadAsStringAsync();
            return responseString;
        }

        [FunctionName("Challenge6_Insert")]
        public static async Task Insert(
            [ActivityTrigger] string combined,
            [CosmosDB(
                databaseName: "sohchallenge6db",
                collectionName: "Orders",
                ConnectionStringSetting = "CosmosDBConnection")]
            IAsyncCollector<Dictionary<string,object>> orderTable,
            ILogger log)
        {
            var jobj = JObject.Parse("{\"data\":" + combined + "}");
            foreach (var e in jobj["data"])
            {
                var values = e.ToObject<Dictionary<string, object>>();
                await orderTable.AddAsync(values);
            }
        }

    }
}
