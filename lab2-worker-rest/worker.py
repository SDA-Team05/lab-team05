import os
import time
import logging
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = os.getenv("MZINGA_API_URL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM")

current_token = None

def get_auth_token():
    """Gets a new authentication token using admin credentials."""
    url = f"{API_URL}/api/users/login"
    payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json().get("token")

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

def api_request(method, endpoint, data=None):
    """Makes an API request. If the token expires (401), logs in again and retries."""
    global current_token
    if not current_token:
        current_token = get_auth_token()
    
    url = f"{API_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {current_token}"}
    
    response = requests.request(method, url, headers=headers, json=data)
    
    if response.status_code == 401:
        logging.info("Token expired. Logging in again...")
        current_token = get_auth_token()
        headers["Authorization"] = f"Bearer {current_token}"
        response = requests.request(method, url, headers=headers, json=data)
        
    response.raise_for_status()
    return response.json()

def resolve_emails(refs):
    """Resolves Payload relationship references to email addresses."""
    if not refs or not isinstance(refs, list):
        return []
    
    emails = []
    for ref in refs:
        # Now value includes the user data directly due to depth=1
        if isinstance(ref, dict) and ref.get("relationTo") == "users":
            user_data = ref.get("value")
            if isinstance(user_data, dict) and "email" in user_data:
                emails.append(user_data["email"])
    return emails


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
        server.send_message(msg, to_addrs=all_recipients)

def run_worker():
    logger.info(f"Worker connected to API. Polling every {POLL_INTERVAL_SECONDS}s...")
    
    while True:
        try:
            # Get pending documents
            query = "/api/communications?where[status][equals]=pending&sort=createdAt&limit=1&depth=1"
            response = api_request("GET", query)
            docs = response.get("docs", [])

            if not docs:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            doc = docs[0]
            doc_id = doc["id"]

            try:
                logger.info(f"Processing Communication ID: {doc_id}")

                # Mark communication status as processing
                api_request("PATCH", f"/api/communications/{doc_id}", {"status": "processing"})

                tos = resolve_emails(doc.get("tos", []))
                ccs = resolve_emails(doc.get("ccs", []))
                bccs = resolve_emails(doc.get("bccs", []))

                html_content = slate_to_html(doc.get("body", []))

                send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

                api_request("PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
                logger.info(f"Successfully sent: {doc_id}")

            except Exception as e:
                logger.error(f"Error processing {doc_id}: {e}")
                api_request("PATCH", f"/api/communications/{doc_id}", {
                    "status": "failed", 
                    "error": str(e)
                })

        except Exception as e:
            logger.error(f"Error during polling loop: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_worker()