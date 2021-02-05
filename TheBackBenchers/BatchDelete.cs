using Azure;
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
        public static void Execute()
        {
            int segmentSize = 10;
            BlobServiceClient service = new BlobServiceClient(Helper.ConnectionString);
            BlobContainerClient container = service.GetBlobContainerClient(Helper.ContainerName);
            container.CreateIfNotExists();
            // Create 100 blobs
            for (int i=0;i <100; i++)
            {
                BlobClient blob = container.GetBlobClient("blob_" + i);
                blob.Upload(new MemoryStream(Encoding.UTF8.GetBytes("Data!")));
            }

            // Call the listing operation and return pages of the specified size.
            var resultSegment = container.GetBlobs()
                .AsPages(null, segmentSize);

            // Enumerate the blobs returned for each page.
            foreach (var blobPage in resultSegment)
            {
                List<Uri> Urilist = new List<Uri>();

                foreach (var blobItem in blobPage.Values)
                {
                    Console.WriteLine("Blob name: {0}", blobItem.Name);
                    Urilist.Add(container.GetBlobClient(blobItem.Name).Uri);
                }

                // Delete blobs at once
                BlobBatchClient batch = service.GetBlobBatchClient();
                Response[] response = null;
                try
                {
                    if (Urilist.Count > 0)
                    {
                        response = batch.DeleteBlobs(Urilist);
                    }
                }
                catch (AggregateException ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                }
            }
        }
    }
}
