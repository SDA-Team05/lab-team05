import os
import time
import logging
from typing import List, Dict, Any, Optional
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

def authenticate(session: requests.Session) -> None:
    """Authenticates the session and updates its headers with the token."""
    url = f"{API_URL}/api/users/login"
    payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    response = session.post(url, json=payload)
    response.raise_for_status()
    
    token = response.json().get("token")
    session.headers.update({"Authorization": f"Bearer {token}"})
    logger.info("Authenticated")


def api_request(session: requests.Session, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
    """Executes an API request with automatic re-authentication on 401 errors."""
    url = f"{API_URL}{endpoint}"
    
    if "Authorization" not in session.headers:
        authenticate(session)

    response = session.request(method, url, json=data)
    
    if response.status_code == 401:
        logger.info("Token expired or invalid. Re-authenticating...")
        authenticate(session)
        response = session.request(method, url, json=data)
        
    response.raise_for_status()
    return response.json() if response.content else {}

def serialize_body(nodes: List[Dict[str, Any]]) -> str:
    """Recursively converts Slate AST nodes to HTML string."""
    html = ""
    for node in nodes:
        if "text" in node:
            text = node["text"]
            if node.get("bold"):
                text = f"<strong>{text}</strong>"
            if node.get("italic"):
                text = f"<em>{text}</em>"
            html += text
            continue

        node_type = node.get("type")
        children_html = serialize_body(node.get("children") or [])

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

def resolve_emails(refs: List[Dict]) -> List[str]:
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

def send_email(to_list: List[str], cc_list: List[str], bcc_list: List[str], subject: str, html_body: str) -> None:
    """Sends email using standard smtplib."""
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    
    msg.attach(MIMEText(html_body, "html"))
    
    all_recipients = to_list + (cc_list or []) + (bcc_list or [])
    
    if not all_recipients:
        logger.warning("No recipients found. Skipping SMTP send.")
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        # Note: Localhost/Mailhog setups (Port 1025) usually don't require login/TLS
        server.send_message(msg, to_addrs=all_recipients)


def process_document(session: requests.Session, doc: Dict[str, Any]) -> None:
    """Processes a single communication document: updates status, resolves emails, serializes body, sends email, and updates status again."""
    doc_id = doc["id"]

    logger.info(f"Processing Communication ID: {doc_id}")

    try:
        api_request(session, "PATCH", f"/api/communications/{doc_id}", {"status": "processing"})

        tos = resolve_emails(doc.get("tos", []))
        ccs = resolve_emails(doc.get("ccs", []))
        bccs = resolve_emails(doc.get("bccs", []))
        
        if not tos:
            raise ValueError("No valid 'to' email addresses found")
        
        body_nodes = doc.get("body", [])

        html_content = serialize_body(body_nodes)
        
        send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

        api_request(session, "PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
        
        logger.info(f"Successfully sent: {doc_id}")

    except Exception as e:
        logger.error(f"Failed to process Communication ID: {doc_id}: {str(e)}")
        
        try:
            api_request(session, "PATCH", f"/api/communications/{doc_id}", {
                "status": "failed", 
                "error": str(e)
            })
        except Exception as patch_err:
            logger.error(f"Failed to update status for Communication ID: {doc_id}: {str(patch_err)}")

def run_worker():
    logger.info(f"Worker connected to API. Polling every {POLL_INTERVAL_SECONDS}s...")
    
    with requests.Session() as session:
        try:
            logger.info("Performing initial API authentication...")
            authenticate(session)
        except Exception as e:
            logger.error(f"Fatal: Could not authenticate with MZinga API: {e}")
            return
        while True:
            try:
                query = "/api/communications?where[status][equals]=pending&sort=createdAt&depth=1"
                response_data = api_request(session, "GET", query)
                docs = response_data.get("docs", [])
                
                if not docs:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                for doc in docs:
                    process_document(session, doc)

            except Exception as e:
                logger.error(f"Error during polling loop: {str(e)}")
                time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_worker()