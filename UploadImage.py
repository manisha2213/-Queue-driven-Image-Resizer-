# UploadImage.py
import os
import logging
import uuid
import base64
import json
from datetime import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient

STORAGE_CONN = os.environ["STORAGE_CONNECTION_STRING"]
UPLOAD_CONTAINER = os.environ.get("BLOB_CONTAINER_UPLOADS", "uploads")
QUEUE_NAME = os.environ.get("IMAGE_QUEUE_NAME", "image-jobs")

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
queue_client = QueueClient.from_connection_string(STORAGE_CONN, QUEUE_NAME)

def save_bytes_to_blob(container_name: str, blob_name: str, data: bytes, content_type: str = None):
    container_client = blob_service.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True, content_settings=None)
    # optionally set content type: blob_client.set_http_headers(content_settings=...)
    account_url = blob_client.url
    return account_url

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("UploadImage HTTP trigger processed a request.")
    try:
        # Support multipart/form-data file upload with field 'file'
        form = req.files if hasattr(req, "files") else None
        file_bytes = None
        file_name = None

        # If multipart (from tools like curl/form)
        try:
            # In Azure Functions, req.get_body() returns raw bytes, and req.files may not be available.
            # We'll try to read JSON/base64 fallback next.
            pass
        except Exception:
            pass

        # Try JSON body with base64: {"filename":"img.jpg","file":"<base64 string>"}
        try:
            body = req.get_json()
        except Exception:
            body = None

        if body and body.get("file"):
            b64 = body["file"]
            file_bytes = base64.b64decode(b64)
            file_name = body.get("filename") or f"{uuid.uuid4().hex}.jpg"

        # If no file provided, return 400
        if not file_bytes:
            # try raw body as bytes (for simple multipart clients you might parse differently)
            raw = req.get_body()
            if raw:
                file_bytes = raw
                file_name = f"{uuid.uuid4().hex}.bin"
            else:
                return func.HttpResponse("No file provided. Send JSON with base64 'file' or raw body.", status_code=400)

        # Build blob path under uploads/ (you could use subfolders)
        blob_name = f"uploads/{datetime.utcnow().strftime('%Y/%m/%d')}/{uuid.uuid4().hex}_{file_name}"

        # Upload to blob
        url = save_bytes_to_blob(UPLOAD_CONTAINER, blob_name, file_bytes)

        # Build queue message
        message = {
            "blobUrl": url,
            "sizes": [320, 1024]
        }
        msg_text = json.dumps(message)
        queue_client.send_message(base64.b64encode(msg_text.encode("utf-8")).decode("utf-8"))

        return func.HttpResponse(json.dumps({"status":"ok","blobUrl":url}), mimetype="application/json")

    except Exception as e:
        logging.exception("Upload failed")
        return func.HttpResponse(f"Error: {e}", status_code=500)
