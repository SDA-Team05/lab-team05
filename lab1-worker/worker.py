import os
import time
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bson import ObjectId
from pymongo import MongoClient, ReturnDocument

# conf vars
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/mzinga?authSource=admin&directConnection=true")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# db setup
# MongoClient handles the authSource and directConnection from the URI string automatically
client = MongoClient(MONGODB_URI)
db = client.get_database() 
comms_col = db.communications
users_col = db.users

def slate_to_html(nodes):
    """Recursively converts Slate AST nodes to HTML string."""
    html = ""
    for node in nodes:
        if "text" in node:
            text = node["text"]
            if node.get("bold"):
                text = f"<b>{text}</b>"
            if node.get("italic"):
                text = f"<i>{text}</i>"
            html += text
            continue

        node_type = node.get("type")
        children_html = slate_to_html(node.get("children", []))

        if node_type == "paragraph":
            html += f"<p>{children_html}</p>"
        elif node_type == "h1":
            html += f"<h1>{children_html}</h1>"
        elif node_type == "h2":
            html += f"<h2>{children_html}</h2>"
        elif node_type == "ul":
            html += f"<ul>{children_html}</ul>"
        elif node_type == "li":
            html += f"<li>{children_html}</li>"
        elif node_type == "link":
            url = node.get("url", "#")
            html += f'<a href="{url}">{children_html}</a>'
        else:
            html += children_html
            
    return html

def resolve_emails(refs):
    """Resolves Payload relationship references to email addresses."""
    if not refs or not isinstance(refs, list):
        return []
    
    ids = []
    for ref in refs:
        if isinstance(ref, dict) and ref.get("relationTo") == "users":
            val = ref.get("value")
            # Force ObjectId conversion
            if isinstance(val, str):
                try:
                    ids.append(ObjectId(val))
                except:
                    continue
            else:
                ids.append(val)
    
    if not ids:
        return []

    # Execute query
    users = users_col.find({"_id": {"$in": ids}}, {"email": 1})
    return [u["email"] for u in users if "email" in u]

def send_email(to_list, cc_list, bcc_list, subject, html_body):
    """Sends email using standard smtplib."""
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    
    msg.attach(MIMEText(html_body, "html"))
    
    all_recipients = to_list + cc_list + bcc_list
    
    if not all_recipients:
        logger.warning("No recipients found. Skipping SMTP send.")
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        # Note: Localhost/Mailhog setups (Port 1025) usually don't require login/TLS
        server.send_message(msg, to_addrs=all_recipients)

def run_worker():
    logger.info(f"Worker connected to MongoDB. Polling every {POLL_INTERVAL_SECONDS}s...")
    
    while True:
        # atomically find one pending and set to processing
        doc = comms_col.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing"}},
            sort=[("createdAt", 1)], # process oldest first
            return_document=ReturnDocument.AFTER
        )

        if not doc:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        try:
            logger.info(f"Processing Communication ID: {doc['_id']}")

            tos = resolve_emails(doc.get("tos", []))
            ccs = resolve_emails(doc.get("ccs", []))
            bccs = resolve_emails(doc.get("bccs", []))

            html_content = slate_to_html(doc.get("body", []))

            send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

            comms_col.update_one({"_id": doc["_id"]}, {"$set": {"status": "sent"}})
            logger.info(f"Successfully sent: {doc['_id']}")

        except Exception as e:
            logger.error(f"Error processing {doc['_id']}: {e}")
            comms_col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"status": "failed", "error": str(e)}}
            )

if __name__ == "__main__":
    run_worker()