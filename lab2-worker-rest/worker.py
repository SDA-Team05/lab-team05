import os
import time
import logging
import requests
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = os.getenv("MZINGA_API_URL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM")

current_token = None

def get_auth_token():
    """Fa il login su MZinga e restituisce il token JWT."""
    url = f"{API_URL}/api/users/login"
    payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json().get("token")

def serialize_slate_to_html(nodes):
    """Converte il formato Slate in HTML (identico al Lab 1)."""
    if not nodes: return ""
    html = ""
    for node in nodes:
        if "text" in node:
            text = node["text"]
            if node.get("bold"): text = f"<strong>{text}</strong>"
            if node.get("italic"): text = f"<em>{text}</em>"
            if node.get("underline"): text = f"<u>{text}</u>"
            html += text
        elif "children" in node:
            children_html = serialize_slate_to_html(node["children"])
            tag = "p"
            if node.get("type") == "h1": tag = "h1"
            elif node.get("type") == "h2": tag = "h2"
            elif node.get("type") == "ul": tag = "ul"
            elif node.get("type") == "ol": tag = "ol"
            elif node.get("type") == "li": tag = "li"
            elif node.get("type") == "link":
                html += f'<a href="{node.get("url")}">{children_html}</a>'
                continue
            html += f"<{tag}>{children_html}</{tag}>"
    return html

def api_request(method, endpoint, data=None):
    """Fa una chiamata API. Se il token scade (401), fa un nuovo login e riprova."""
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

def process_communication(doc):
    """Invia fisicamente l'email tramite MailHog."""
    doc_id = doc["id"]
    logging.info(f"Processing Communication ID: {doc_id}")
    
    api_request("PATCH", f"/api/communications/{doc_id}", {"status": "processing"})
    
    try:
        msg = EmailMessage()
        msg['Subject'] = doc.get("subject", "No Subject")
        msg['From'] = EMAIL_FROM
        
        def extract_emails(field):
            return [item["value"]["email"] for item in doc.get(field, []) if "value" in item and "email" in item["value"]]
            
        tos = extract_emails("tos")
        if not tos:
            raise ValueError("No recipients found in 'tos'")
            
        msg['To'] = ", ".join(tos)
        
        ccs = extract_emails("ccs")
        if ccs: msg['Cc'] = ", ".join(ccs)
        
        bccs = extract_emails("bccs")
        if bccs: msg['Bcc'] = ", ".join(bccs)

        html_body = serialize_slate_to_html(doc.get("body", []))
        msg.set_content("Please view this email in an HTML compatible client.")
        msg.add_alternative(html_body, subtype='html')

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.send_message(msg)
            
        api_request("PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
        logging.info(f"Successfully sent: {doc_id}")
        
    except Exception as e:
        logging.error(f"Error processing {doc_id}: {e}")
        api_request("PATCH", f"/api/communications/{doc_id}", {"status": "failed"})

def run_worker():
    global current_token
    try:
        current_token = get_auth_token()
        logging.info("Worker authenticated successfully. Polling API...")
    except Exception as e:
        logging.error(f"Failed initial login: {e}")
        return

    while True:
        try:
            response = api_request("GET", "/api/communications?where[status][equals]=pending&depth=1")
            docs = response.get("docs", [])
            
            for doc in docs:
                process_communication(doc)
                
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logging.error(f"Error during polling: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run_worker()