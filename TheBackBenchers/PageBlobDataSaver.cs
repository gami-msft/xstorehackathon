using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;

namespace Hackathon
{
    class PageBlobDataSaver
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating Page Blob Data saver scenario. \n");

            // Set output path for debugging purposes
            Helper helper = new Helper();
            helper.SetConsoleOutPutPath(redirectOutputToFile, ".\\PageBlobDataSaver.txt");

            // Set the Source Data
            Random random = new Random();
            Byte[] sourceBytes = new Byte[Helper.Kilobyte];
            random.NextBytes(sourceBytes);

            // Create a Blob Service Client
            BlobServiceClient blobServiceClient = new BlobServiceClient(Helper.ConnectionString);

            var blobContainerClient =
                blobServiceClient.GetBlobContainerClient(Helper.ContainerName);
            blobContainerClient.CreateIfNotExists();

            var pageBlobClient = blobContainerClient.GetPageBlobClient("samplepageblob.vhd");

            //Create 16 Gb page blob
            pageBlobClient.Create(16 * Helper.Gigabyte);

            //Upload pages
            for (int i = 0; i < 100; i++)
            {
                //Upload Pages at random offsets
                long offset = random.Next(Helper.Megabyte) * 512;
                Console.WriteLine("Uploading pages at random offset:" + offset);

                pageBlobClient.UploadPages(new MemoryStream(sourceBytes), offset);
            }

            // Get Page ranges provides a list of Ranges which are backed by data
            // Only non empty page ranges will be provided 
            // This helps to ensure that unnecessary calls to server are saved.
            IEnumerable<HttpRange> pageRanges = pageBlobClient.GetPageRanges().Value.PageRanges;

            //Download those pages
            foreach (var range in pageRanges)
            {
                Console.WriteLine("Dowloading from valid range:" + range);
                var pageBlob = pageBlobClient.Download(range);
                // Assert that the page downloaded is non empty
                Assert.IsTrue(pageBlob.Value.ContentLength > 0);
            }

            Console.WriteLine("Code demonstrating Page Blob Data saver scenario completed. \n");
        }
    }
}
