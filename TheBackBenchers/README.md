# Project submission for 'XStore-Hackathon'

## Team Details

* Team - The Back Benchers
* Teammates - Gaurav Mishra (gami@microsoft.com), Sneha Varma (snvarma@microsoft.com)


## Program Details (as they appear in demo)

* Create Million Blobs
* List Million Blobs (sequential listing)
* List Million Blobs Randomly (multi threaded)
* Throttling Sample
* Page Blob Data Saver
* Batch Delete

## How To Run

* Update the details asked for in AccountDetails.xml file.
* Open the .net project {Hackathon.csproj} in Visual Studio (>2017) and press Ctrl + F5 to launch the project.
* Follow on screen instructions [We will ask you to select a problem number and run the same for you].
* Please note that in 'Throttling' sample we hit ingress throttling limit of 60 MBps for a page blob. Hence make sure that the machine you are running this code on has > 70 MBps internet connection. [e.g. VMs used for XStore Dev work]