using Azure.Storage.Files.DataLake;
using System;

namespace Hackathon
{
    class CreateMillionBlobs
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating creating a million blobs [via parallel tasks].");

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, "C:\\Users\\gami\\Desktop\\Hackathon\\CreateMillionBlobs.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS client
            DataLakeServiceClient serviceClient = helper.GetDataLakeServiceClient(Helper.storageAccountName, Helper.clientId,
                Helper.clientSecret, Helper.tenantId);

            // Create the specified filesystem if it does not exist
            helper.CreateFileSystem(serviceClient, Helper.containerName);
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(Helper.containerName);

            // Create a directory inside the container
            string directoryName = "Test";
            helper.CreateDirectory(serviceClient, Helper.containerName, directoryName);

            // Create 1 million Blobs
            helper.CreateMillionBlobs(fileSystemClient, directoryName);
        }
    }
}
