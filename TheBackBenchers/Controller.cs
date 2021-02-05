using System;

namespace Hackathon
{
    enum ProblemName
    {
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

            // Controls what code we want to run, please select 
            // problem name from the specified enum
            ProblemName runChallenge = ProblemName.BatchDelete;

            switch(runChallenge)
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
