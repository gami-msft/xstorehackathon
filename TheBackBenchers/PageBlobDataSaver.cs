using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.IO;

namespace Hackathon
{
    class PageBlobDataSaver
    {
        public static void Execute(bool redirectOutputToFile)
        {
            // Set output path for debugging purposes
            Helper helper = new Helper();
            helper.SetConsoleOutPutPath(redirectOutputToFile, ".\\PageBlobDataSaver.txt");

            Random random = new Random();
            Byte[] sourceBytes = new Byte[Helper.Kilobyte];
            random.NextBytes(sourceBytes);

            BlobServiceClient blobServiceClient = new BlobServiceClient(Helper.ConnectionString);

            var blobContainerClient =
                blobServiceClient.GetBlobContainerClient(Helper.ContainerName);
            blobContainerClient.CreateIfNotExists();

            var pageBlobClient = blobContainerClient.GetPageBlobClient("samplepageblob.vhd");

            //Create 16 gb page blob
            pageBlobClient.Create(16 * Helper.Gigabyte);

            //Upload pages
            for (int i = 0; i < 100; i++)
            {
                //Upload Pages at random offsets
                long offset = random.Next(Helper.Megabyte) * 512;
                Console.WriteLine("Uploading at random offset:" + offset);

                pageBlobClient.UploadPages(new MemoryStream(sourceBytes), offset);
            }

            // Get Page ranges which have data
            IEnumerable<HttpRange> pageRanges = pageBlobClient.GetPageRanges().Value.PageRanges;

            //Download those pages
            foreach (var range in pageRanges)
            {
                Console.WriteLine("Dowloading from valid range:" + range);
                var pageBlob = pageBlobClient.Download(range);
            }
        }
    }
}
