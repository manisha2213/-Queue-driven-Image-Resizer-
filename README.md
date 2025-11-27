Queue-Driven Image Resizer using Azure Functions, Blob Storage & Queues
Project Overview :

This project implements an asynchronous, queue-driven image processing pipeline using: Azure Functions (Python) Azure Storage Queues Azure Blob Storage Pillow (PIL)

A client uploads an image via an HTTP endpoint (/api/upload)
The image is stored in Blob Storage under uploads/
A message is pushed into the Storage Queue image-jobs
A Queue Trigger Function picks the message
The worker:

downloads the original
resizes it to target dimensions
saves resized images under resized
generates a JSON log
writes the log into function-logs
Azure Components Used

Blob Storage containers :

uploads - original uploaded images
resized - resized images
function-logs - pipeline execution logs
Queue :

Name : image-jobs
Azure Functions

upload_api - HTTP trigger
queue_processor - Storage Queue trigger
Tech Stack

Azure Functions
Azure Storage Blob
Azure Storage Queue
Python
Pillow (PIL)
Local Development Setup :

Initially, create a new azure project
Install dependencies from requirements.txt file
Configure local.settings.json
Run locally func start
Testing the API :

HTTP Upload

upload_api: [POST] http://localhost:7071/api/upload
Response Sample:

{ "message": "uploaded", "blobUrl": "" }

Deployment Steps

From VS Code:

Azure icon → Deploy to Function App → Select subscription → Select Function App
Then configure AzureWebJobsStorage in portal under:
Configuration → Application Settings
Restart App.
