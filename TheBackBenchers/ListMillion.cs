using Azure.Storage.Files.DataLake;
using System;

namespace Hackathon
{
    class ListMillion
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating listing a million blobs [in sequential order].");

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, "C:\\Users\\gami\\Desktop\\Hackathon\\ListMillion.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS client
            DataLakeServiceClient serviceClient = helper.GetDataLakeServiceClient(Helper.StorageAccountName, Helper.ClientId,
                Helper.ClientSecret, Helper.TenantId);

            // Get reference to an existing filesystem
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(Helper.ContainerName);

            // List 1 million Blobs [note send recursive = true here]
            helper.ListBlobs(fileSystemClient, Helper.DirectoryForListing, true, false).GetAwaiter().GetResult();
        }
    }
}
