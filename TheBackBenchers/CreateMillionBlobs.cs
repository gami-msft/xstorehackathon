using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Hackathon
{
    class CreateMillionBlobs
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating creating a million blobs [via parallel tasks].");

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, ".\\CreateMillionBlobs.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS client
            DataLakeServiceClient serviceClient = helper.GetDataLakeServiceClient(helper.StorageAccountName, helper.ClientId,
                helper.ClientSecret, helper.TenantId);

            // Create the specified filesystem if it does not exist
            helper.CreateFileSystem(serviceClient, helper.ContainerName);
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(helper.ContainerName);

            // Create a directory inside the container
            helper.CreateDirectory(serviceClient, helper.ContainerName, helper.DirectoryForCreatingMillionBlobs);

            // Create 1 million Blobs
            CreateMillionBlobsInAContainer(fileSystemClient, helper.DirectoryForCreatingMillionBlobs);

            Console.WriteLine("1 million files have been created successfully.");
        }

        /// <summary>
        /// Main function we can use to create 1 million 
        /// empty block blobs inside a given directory
        /// </summary>
        public static void CreateMillionBlobsInAContainer(DataLakeFileSystemClient fileSystemClient, string directoryName)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directoryName);

            // Use the service client to create Blobs of 
            // the format 000000 to 999999 .txt names
            // Initiate 1000 tasks creating 1000 files each
            // Currently file naming is based on the fact that 
            // numTasks = numFilesPerTask (also set D* value accordingly)

            int numTasks = 1000;
            int numFilesPerTask = 1000;

            List<TaskAwaiter> allTasks = new List<TaskAwaiter>();

            for (int i = 0; i < numTasks; i++)
            {
                Console.WriteLine("Creating Task Id = {0}.", i);
                Helper helper = new Helper();
                allTasks.Add(helper.UploadFile(directoryClient, numTasks, i, numFilesPerTask).GetAwaiter());
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
                    Console.WriteLine("Task Id {0}, failed with -\n", i);
                    Console.WriteLine(e.Message);
                }
            }
        }
    }
}
