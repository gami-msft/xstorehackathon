using Azure.Storage.Files.DataLake;
using System;

namespace Hackathon
{
    class ListMillionRandom
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating listing a million blobs [in random order].");

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, "C:\\Users\\gami\\Desktop\\Hackathon\\ListMillionRandom.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS client
            DataLakeServiceClient serviceClient = helper.GetDataLakeServiceClient(Helper.storageAccountName, Helper.clientId,
                Helper.clientSecret, Helper.tenantId);

            // Get reference to an existing filesystem
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(Helper.containerName);

            // List 1 million Blobs in random order
            helper.ListMillionBlobsInRandomOrder(fileSystemClient, Helper.directoryForListing);
        }
    }
}
