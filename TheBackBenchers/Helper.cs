using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Hackathon
{
    class Helper
    {
        // Common arguments which are to be used for running the code [will remain common]
        // Please set them according to the storage account / SP you are using 
        public const string tenantId = "";
        public const string clientId = "";
        public const string clientSecret = "";
        public const string storageAccountName = "";
        public const string blobStorageAccountName = "";
        public const string containerName = "";
        public const string directoryForListing = "";
        public const long OneGigabyteAsBytes = 1024 * 1024 * 1024;

        // Public variables to store results if required
        List<string> directoryList = new List<string> ();
        List<string> fileList = new List<string>();

        /// <summary>
        /// Create a container
        /// </summary>
        public void CreateFileSystem(DataLakeServiceClient serviceClient, string containerName)
        {
            // Create the filesystem and swallow already exists error
            Console.WriteLine("Creating filesystem = {0}.", containerName);

            try
            {
                var r = serviceClient.CreateFileSystemAsync(containerName).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                // Do not throw on already exists exception
                if (e.Message.Contains("ContainerAlreadyExists"))
                {
                    Console.WriteLine("Filesystem already exists.");
                }
                else
                {
                    throw e;
                }
            }
        }

        /// <summary>
        /// Create a directory
        /// </summary>
        public void CreateDirectory(DataLakeServiceClient serviceClient, 
            string containerName, string directoryName)
        {
            Console.WriteLine("Creating directory = {0}.", directoryName);
            var directoryClient = serviceClient.GetFileSystemClient(containerName).GetDirectoryClient(directoryName);
            directoryClient.CreateAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Function to list all blobs inside a directory also
        /// stores the ouput in lists if required
        /// </summary>
        public async Task ListBlobs(DataLakeFileSystemClient fileSystemClient, string directoryName,
            bool recursive, bool storeOutput)
        {
            IAsyncEnumerator<PathItem> enumerator =
                fileSystemClient.GetPathsAsync(directoryName, recursive).GetAsyncEnumerator();

            await enumerator.MoveNextAsync();

            PathItem item = enumerator.Current;

            while (item != null)
            {
                Console.WriteLine(item.Name);

                // Store directories
                if (storeOutput == true && item.IsDirectory == true)
                {
                    this.directoryList.Add(item.Name);
                }

                // Store files
                if (storeOutput == true && item.IsDirectory == false)
                {
                    this.fileList.Add(item.Name);
                }

                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }
        }

        /// <summary>
        /// Function to list one million blobs in random order
        /// Parrallelization can be controlled via 'numTasks'
        /// </summary>
        public void ListMillionBlobsInRandomOrder(DataLakeFileSystemClient fileSystemClient, string directoryName)
        {
            // Assuming that listing is being done on a directory that has 
            // million blobs which have been partitioned [into subdirectories]

            // Step 1- Perform a non recursive listing on the directory 
            // and capture all those directories in a List<>
            Console.WriteLine("Performing a non recursive listing on top level.");
            this.ListBlobs(fileSystemClient, directoryName, false, true).GetAwaiter().GetResult();

            // Step 2- Chunk down above list into buckets of 'numTasks' and let 
            // them print the output of listing in a unique directory
            int numTasks = 1000;
            List<string> temp = new List<string>();

            // Step 3- Initiate recursive listing on all the buckets in parallel
            Console.WriteLine("Initiating {0} parallel tasks to provide randomness in listing.",
                numTasks);

            int i = 0;
            while (i < this.directoryList.Count)
            {
                List<TaskAwaiter> allTasks = new List<TaskAwaiter>();

                for (int j = 0; j < numTasks; j++)
                {
                    allTasks.Add(this.ListBlobs(fileSystemClient, this.directoryList[i],
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
                        Console.WriteLine("Task Id {0}, failed with -\n", i);
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }

        /// <summary>
        /// Main function we can use to create 1 million 
        /// empty block blobs inside a given directory
        /// </summary>
        public void CreateMillionBlobs(DataLakeFileSystemClient fileSystemClient, string directoryName)
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
                Helper uploadFiles = new Helper();
                allTasks.Add(uploadFiles.UploadFile(directoryClient, numTasks, i, numFilesPerTask).GetAwaiter());
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

        /// <summary>
        /// Get a service client for ADLS Gen2 and use AAD token for authorization
        /// </summary>
        public DataLakeServiceClient GetDataLakeServiceClient(string accountName, string clientID, 
            string clientSecret, string tenantID, DataLakeClientOptions options = null)
        {
            TokenCredential credential = new ClientSecretCredential(
                tenantID, clientID, clientSecret, new TokenCredentialOptions());

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            if (options == null)
            {
                return new DataLakeServiceClient(new Uri(dfsUri), credential);
            }
            else
            {
                return new DataLakeServiceClient(new Uri(dfsUri), credential, options);
            }
        }

        /// <summary>
        /// Get a service client for ADLS Gen2 [Blob endpoint] and use AAD token for authorization
        /// </summary>
        public BlobServiceClient GetBlobServiceClient(string accountName, string clientID,
            string clientSecret, string tenantID, BlobClientOptions options = null)
        {
            TokenCredential credential = new ClientSecretCredential(
                tenantID, clientID, clientSecret, new TokenCredentialOptions());

            string dfsUri = "https://" + accountName + ".blob.core.windows.net";

            if (options == null)
            {
                return new BlobServiceClient(new Uri(dfsUri), credential);
            }
            else
            {
                return new BlobServiceClient(new Uri(dfsUri), credential, options);
            }
        }

        /// <summary>
        /// A Task() which we will use for uploading a million blobs in parallel 
        /// </summary>
        public async Task UploadFile(DataLakeDirectoryClient directoryClient, int numTasks, int taskId, int numFilesPerTask)
        {
            for (int i = 0; i < numFilesPerTask; i++)
            {
                // Giving unique name to each path
                string fileName = "Dir_" + i.ToString("D3") + "/" + ((taskId * numTasks) + i).ToString("D6");
                
                // Send request to create the file
                Console.WriteLine("Creating file = {0}.", fileName);
                DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(fileName);
            }
        }

        /// <summary>
        /// A function which will upload pages to a specified page blob
        /// </summary>
        public async Task UpdatePageBlob(PageBlobClient pageBlobClient, int clientId)
        {
            // Get random 4 MB of data
            Random random = new Random();
            Byte[] sourceBytes = new Byte[1024 * 1024 * 4];
            random.NextBytes(sourceBytes);

            // Upload the data to the page blob at a random offset
            long offset = random.Next(1024 * 1024) * 512;
            Console.WriteLine("Uploading data to page blob, client: " + clientId.ToString());
 
            // Upload 40 MB of data
            int maxRequests = 10;
            while (maxRequests-- > 0)
            {
                try
                {
                    await pageBlobClient.UploadPagesAsync(new MemoryStream(sourceBytes), offset);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Client {0}, failed to upload the data {1}", clientId, e.Message);
                    throw e;
                }
            }
        }


        /// <summary>
        /// Utility method to redirect console output to a file if required
        /// </summary>
        public void SetConsoleOutPutPath(bool redirectConsoleOutputToFile = false, string path = null)
        {
            if (redirectConsoleOutputToFile)
            {
                // Set output file path to local desktop file
                FileStream filestream = new FileStream(path, FileMode.Create);
                var streamwriter = new StreamWriter(filestream);
                streamwriter.AutoFlush = true;
                Console.SetOut(streamwriter);
                Console.SetError(streamwriter);
            }
        }
    }
}
