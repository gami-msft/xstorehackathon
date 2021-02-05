using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hackathon
{
    class Controller
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Xstore Hackathon !!!");

            // Controls what code we want to run, uncomment 
            // any one of these below to see the code in action

            // Flag to toggle printing output 
            // to console [false] or a file [true] 
             bool redirectOutputToFile = false;

            // Create 1 million blobs
            // CreateMillionBlobs.Execute(redirectOutputToFile);

            // List 1 million blobs
           //  ListMillion.Execute(redirectOutputToFile);

            // List 1 million blobs in random order
            // ListMillionRandom.Execute(redirectOutputToFile);

            // Throttling Sample
            // Throttling.Execute(redirectOutputToFile);


            // PageBlobDataSaver Sample
            //PageBlobDataSaver.Execute();

            // BatchDelete Sample
            BatchDelete.Execute();
        }
    }
}
