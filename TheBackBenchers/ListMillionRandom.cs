using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Hackathon
{
    class ListMillionRandom
    {
        public static void Execute(bool redirectOutputToFile)
        {
            Console.WriteLine("Code demonstrating listing a million blobs [in random order].");

            Helper helper = new Helper();

            // Set output path for debugging purposes
            helper.SetConsoleOutPutPath(redirectOutputToFile, ".\\ListMillionRandom.txt");

            // Get the token from AAD for the SP
            // and use it to get the ABFS client
            DataLakeServiceClient serviceClient = helper.GetDataLakeServiceClient(Helper.StorageAccountName, Helper.ClientId,
                Helper.ClientSecret, Helper.TenantId);

            // Get reference to an existing filesystem
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(Helper.ContainerName);

            // List 1 million Blobs in random order
            ListMillionBlobsRandomOrder(fileSystemClient, Helper.DirectoryForListing);

            Console.WriteLine("Code demonstrating listing a million blobs in random order [completed successfully].");
        }

        /// <summary>
        /// Function to list one million blobs in random order
        /// Parrallelization can be controlled via 'numTasks'
        /// </summary>
        public static void ListMillionBlobsRandomOrder(DataLakeFileSystemClient fileSystemClient, string directoryName)
        {
            // Assuming that listing is being done on a directory that has 
            // million blobs which have been partitioned [into subdirectories]

            // Step 1- Perform a non recursive listing on the directory 
            // and capture all those directories in a List<>
            Helper helper = new Helper();

            Console.WriteLine("Performing a non recursive listing on top level.");
            helper.ListBlobs(fileSystemClient, directoryName, false, true).GetAwaiter().GetResult();

            // Step 2- Chunk down above list into buckets of 'numTasks' and let 
            // them print the output of listing in a unique directory
            int numTasks = 1000;
            List<string> temp = new List<string>();

            // Step 3- Initiate recursive listing on all the buckets in parallel
            Console.WriteLine("Initiating {0} parallel tasks to provide randomness in listing.",
                numTasks);

            int i = 0;
            while (i < helper.directoryList.Count)
            {
                List<TaskAwaiter> allTasks = new List<TaskAwaiter>();

                for (int j = 0; j < numTasks; j++)
                {
                    allTasks.Add(helper.ListBlobs(fileSystemClient, helper.directoryList[i],
                        true, false).GetAwaiter()); ;
                    i++;
                }

                // Wait on all tasks to get finished
                for (int j = 0; j < allTasks.Count; j++)
                {
                    try
                    {
                        allTasks[j].GetResult();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Task Id {0}, failed with -\n", j);
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }
    }
}
