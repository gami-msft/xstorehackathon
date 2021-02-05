using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Hackathon
{
    class BatchDelete
    {
        public static void Execute(bool redirectOutputToFile)
        {
            // Set output path for debugging purposes
            Helper helper = new Helper();
            helper.SetConsoleOutPutPath(redirectOutputToFile, ".\\BatchDelete.txt");

            int segmentSize = 10;
            // Set up clients
            BlobServiceClient blobServiceClient = new BlobServiceClient(Helper.ConnectionString);
            BlobContainerClient container1 = blobServiceClient.GetBlobContainerClient(Helper.ContainerName);
            BlobContainerClient container2 = blobServiceClient.GetBlobContainerClient(Helper.ContainerName + "2");
            BlobBatchClient batch = blobServiceClient.GetBlobBatchClient();

            container1.CreateIfNotExists();
            container2.CreateIfNotExists();

            Console.WriteLine("Creating some sample blobs for deletion.");

            // Create 100 blobs in both the containers
            for (int i = 0;i < 100; i++)
            {
                BlobClient blob1 = container1.GetBlobClient("blob_" + i);
                blob1.Upload(new MemoryStream(Encoding.UTF8.GetBytes("Data!")));

                BlobClient blob2 = container2.GetBlobClient("blob_" + i);
                blob2.Upload(new MemoryStream(Encoding.UTF8.GetBytes("Data!")));
            }

            // Call the listing operation and enumerate the result segment.
            var containerListResult =
                blobServiceClient.GetBlobContainers()
                .AsPages(null, segmentSize);

            foreach (var containerPage in containerListResult)
            {
                foreach (var containerItem in containerPage.Values)
                {
                    Console.WriteLine("Container name: {0}", containerItem.Name);
                    // Call the listing operation and return pages of the specified size.
                    var containerClient = blobServiceClient.GetBlobContainerClient(containerItem.Name);

                    var resultSegment = containerClient.GetBlobs()
                        .AsPages(null, segmentSize);

                    // Enumerate the blobs returned for each page.
                    foreach (var blobPage in resultSegment)
                    {
                        List<Uri> Urilist = new List<Uri>();

                        foreach (var blobItem in blobPage.Values)
                        {
                            Console.WriteLine("Adding Blob to delete queue: {0}", blobItem.Name);
                            Urilist.Add(containerClient.GetBlobClient(blobItem.Name).Uri);
                        }

                        // Delete blobs at once
                        try
                        {
                            if (Urilist.Count > 0)
                            {
                                var response = batch.DeleteBlobs(Urilist);
                            }
                        }
                        catch (AggregateException ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }
                }
            }

            Console.WriteLine("All the blobs in the account are deleted successfully.");
        }
    }
}
