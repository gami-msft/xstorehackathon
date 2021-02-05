using System;
using System.Xml;

namespace Hackathon
{
    enum ProblemName
    {
        None,
        CreateMillionBlobs,
        ListMillion,
        ListMillionRandom,
        Throttling,
        PageBlobDataSaver,
        BatchDelete
    };

    class Controller
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Xstore Hackathon !!!");

            // Flag to toggle output redirection 
            // to console [false] or a file [true] 
             bool redirectOutputToFile = false;

            Console.WriteLine("Enter the Problem id to execute." +
                " \n 1 for CreateMillionBlobs" +
                " \n 2 for ListMillion" +
                " \n 3 for ListMillionRandom" +
                " \n 4 for Throttling" +
                " \n 5 for PageBlobDataSaver" +
                " \n 6 for BatchDelete");

            // Controls what code we want to run
            // depending on problem id specified
            string val = Console.ReadLine();

            // Convert to ProblemName
            ProblemName runChallenge = (ProblemName)Convert.ToInt16(val);

            switch (runChallenge)
            {
                case ProblemName.CreateMillionBlobs:

                    // Create 1 million blobs
                    CreateMillionBlobs.Execute(redirectOutputToFile);
                    break;

                case ProblemName.ListMillion:

                    // List 1 million blobs
                    ListMillion.Execute(redirectOutputToFile);
                    break;

                case ProblemName.ListMillionRandom:

                    // List 1 million blobs in random order
                    ListMillionRandom.Execute(redirectOutputToFile);
                    break;

                case ProblemName.Throttling:

                    // Program to demonstrate 503 and how to handle the same
                    Throttling.Execute(redirectOutputToFile);
                    break;

                case ProblemName.PageBlobDataSaver:

                    // Program to demonstrate 503 and how to handle the same
                    PageBlobDataSaver.Execute(redirectOutputToFile);
                    break;

                case ProblemName.BatchDelete:

                    // Program to demonstrate BatchDelete api
                    BatchDelete.Execute(redirectOutputToFile);
                    break;

                default:

                    // Invalid problem name
                    Console.WriteLine("Problem name is invalid, please provide a valid problem name!");
                    break;
            }
        }
    }
}
