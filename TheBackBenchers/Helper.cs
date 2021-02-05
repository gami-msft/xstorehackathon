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
        // Common arguments which are to be used for running the code
        // Please set them according to the storage account / SP you 
        // are using, Please leave these empty while checking in
        
        public const string TenantId = "";
        public const string ClientId = "";
        public const string ClientSecret = "";
        
        public const string StorageAccountName = "";
        public const string BlobStorageAccountName = "";
        public const string ConnectionString = "";

        // Constants which are used at multiple places but can be fixed
        public const int Kilobyte = 1024;
        public const int Megabyte = 1024 * Kilobyte;
        public const long Gigabyte = 1024 * Megabyte;
        public const string ContainerName = "millionblobs";
        public const string DirectoryForListing = "MultipleLevels";

        // Public variables to store results if required
        public List<string> directoryList = new List<string> ();
        public List<string> fileList = new List<string>();

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
