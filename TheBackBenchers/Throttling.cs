using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Hackathon
{
    class Throttling
    {
        /// We are writing a simple application here 
        /// which demonstrates a simple way through which 
        /// customers can implement retry logic in order to 
        /// have scalability or fault tolerance in their systems.

        /// Application Scenario - 
        /// Let us consider a client scenario where we want to upload pages [~40 MB]
        /// to a specific page blob property in quick successions
        /// and we have multiple such clients [~500] {a possible reason to do 
        /// so would be e.g. upload daily briefing from multiple clients to this page blob}

        /// From Azure Storage documentation we can see that 
        /// operations to a page blob are capped to ~60MBPs ingress limit
        /// so it is possible that we will face 503 (server busy) from the server
        /// if all clients are uploading to the page blob simultaneously
        /// Ref Doc - https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets

        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating a throttling scenario. \n");

            /// See the difference when we run the exact same piece of code
            /// There are two ways we can run this code, select
            /// 'Option 1' Fixed rery policy [We will see that app will fail with many 503's]
            /// 'Option 2' Exponential backoff policy [We will see our app will succeed].
            
            int selectedOption = 2;
            BlobClientOptions options = new BlobClientOptions();
            options.Retry.MaxRetries = 10;

            if (selectedOption == 1)
            {
                options.Retry.Mode = Azure.Core.RetryMode.Fixed;
            }
            else
            {
                options.Retry.Mode = Azure.Core.RetryMode.Exponential;
            }

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, "C:\\Users\\gami\\Desktop\\Hackathon\\Trottling.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS [Blob] client
            BlobServiceClient serviceClient = helper.GetBlobServiceClient(Helper.blobStorageAccountName, Helper.clientId,
                Helper.clientSecret, Helper.tenantId, options);

            var blobContainerClient =
                serviceClient.GetBlobContainerClient(Helper.containerName);

            // Get a page blob client
            var pageBlobClient = blobContainerClient.GetPageBlobClient("dailyUpdates" + DateTime.Now.ToString()
                + ".vhd");
            pageBlobClient.Create(16 * Helper.OneGigabyteAsBytes);

            // Let us simulate 500 clients simultaneously 
            // trying to update the page blob

            List<TaskAwaiter> allTasks = new List<TaskAwaiter>();
            int numClients = 500;

            for (int i = 0; i < numClients; i++)
            {
                Console.WriteLine("Creating Client Id = {0}.", i);
                allTasks.Add(helper.UpdatePageBlob(pageBlobClient, i).GetAwaiter());
            }

            // Wait on all tasks to get finished
            for (int i = 0; i < allTasks.Count; i++)
            {
                try
                {
                    allTasks[i].GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Client Id {0}, failed with -\n", i);
                    Console.WriteLine(e.Message);
                    throw e;
                }
            }

            Console.WriteLine("\n\n All Tasks completed successfully !!");
        }
    }
}
