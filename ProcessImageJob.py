# ProcessImageJob.py
import os
import json
import base64
import logging
import time
from datetime import datetime
from io import BytesIO
from PIL import Image
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.queue import QueueServiceClient

STORAGE_CONN = os.environ["STORAGE_CONNECTION_STRING"]
RESIZED_CONTAINER = os.environ.get("BLOB_CONTAINER_RESIZED", "resized")
LOG_CONTAINER = "function-logs"
QUEUE_NAME = os.environ.get("IMAGE_QUEUE_NAME", "image-jobs")
POISON_QUEUE = os.environ.get("POISON_QUEUE_NAME", "image-jobs-poison")

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
queue_service = QueueServiceClient.from_connection_string(STORAGE_CONN)
poison_queue_client = queue_service.get_queue_client(POISON_QUEUE)

def download_blob_to_bytes(url: str):
    # Simplest: reuse blob client by parsing url. BlobServiceClient can get blob client by container & blob name.
    # We'll parse the path after container name.
    # URL format: https://<account>.blob.core.windows.net/<container>/<path>
    parts = url.split('/')
    container = parts[3]
    blob_path = '/'.join(parts[4:])
    client = blob_service.get_container_client(container).get_blob_client(blob_path)
    stream = client.download_blob()
    return stream.readall(), client

def upload_bytes(container_name: str, blob_name: str, data: bytes, content_type: str = None):
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass
    blob = container.get_blob_client(blob_name)
    cs = ContentSettings(content_type=content_type) if content_type else None
    blob.upload_blob(data, overwrite=True, content_settings=cs)
    return blob.url

def generate_log_blob(logobj: dict):
    container = blob_service.get_container_client(LOG_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    datepath = datetime.utcnow().strftime("%Y/%m/%d")
    blobname = f"ImageResizer/{datepath}/{uuid4hex()}.json"
    blob = container.get_blob_client(blobname)
    blob.upload_blob(json.dumps(logobj), overwrite=True)
    return blob.url

def uuid4hex():
    import uuid
    return uuid.uuid4().hex

def resize_image_bytes(img_bytes: bytes, width: int):
    with Image.open(BytesIO(img_bytes)) as img:
        orig_format = img.format or "JPEG"
        # maintain aspect ratio
        wpercent = (width / float(img.size[0]))
        hsize = int((float(img.size[1]) * float(wpercent)))
        img = img.resize((width, hsize), Image.LANCZOS)
        out = BytesIO()
        img.save(out, format=orig_format)
        return out.getvalue(), orig_format

def move_to_poison(queue_message_text: str):
    # push the message text to poison queue so it's saved for manual inspection
    try:
        poison_queue_client.send_message(base64.b64encode(queue_message_text.encode('utf-8')).decode('utf-8'))
    except Exception:
        logging.exception("failed to move to poison queue")

def main(msg: func.QueueMessage):
    start = time.time()
    try:
        # The message might be base64-encoded (we sent it encoded). Decode.
        encoded = msg.get_body().decode('utf-8')
        try:
            decoded = base64.b64decode(encoded).decode('utf-8')
        except Exception:
            # maybe sent plain JSON
            decoded = encoded

        payload = json.loads(decoded)
        blob_url = payload["blobUrl"]
        sizes = payload.get("sizes", [320, 1024])

        blob_bytes, orig_blob_client = download_blob_to_bytes(blob_url)
        output_urls = []

        for size in sizes:
            resized_bytes, fmt = resize_image_bytes(blob_bytes, size)
            # store under resized/<size>/<originalfilename>
            # extract original filename from blob URL
            parts = blob_url.split('/')
            original_name = parts[-1]
            dest_blob_name = f"resized/{size}/{original_name}"
            url = upload_bytes(RESIZED_CONTAINER, dest_blob_name, resized_bytes, content_type=f"image/{fmt.lower()}")
            output_urls.append(url)

        # log
        logobj = {
            "originalUrl": blob_url,
            "outputUrls": output_urls,
            "status": "success",
            "processingTimeSeconds": time.time() - start,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        generate_log_blob(logobj)
        logging.info("Processed image job for %s", blob_url)

    except Exception as e:
        logging.exception("Processing failed: %s", e)
        # Dequeue count check and poison handling:
        try:
            dc = msg.dequeue_count  # azure.functions.QueueMessage property
        except Exception:
            dc = None

        # When msg.dequeue_count is > 5, move to poison queue to inspect later
        if dc and dc >= 5:
            move_to_poison(msg.get_body().decode('utf-8'))
            logging.error("Moved to poison queue after %s attempts", dc)
            # Optionally delete the message (Functions host will do so if function completes without exception)
        # Re-raise to let Functions runtime retry (unless we moved to poison)
        raise
