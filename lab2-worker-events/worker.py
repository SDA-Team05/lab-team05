import asyncio
import os
import json
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import aio_pika
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
ROUTING_KEY = os.environ["ROUTING_KEY"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
API_URL = os.getenv("MZINGA_URL")
ADMIN_EMAIL = os.getenv("MZINGA_EMAIL")
ADMIN_PASSWORD = os.getenv("MZINGA_PASSWORD")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")

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

def api_request(method, endpoint, data=None, params=None):
    """Makes an API request. If the token expires (401), logs in again and retries."""
    global current_token
    if not current_token:
        current_token = get_auth_token()
    
    url = f"{API_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {current_token}"}
    
    response = requests.request(method, url, headers=headers, json=data, params=params)
    
    if response.status_code == 401:
        logging.info("Token expired. Logging in again...")
        current_token = get_auth_token()
        headers["Authorization"] = f"Bearer {current_token}"
        response = requests.request(method, url, headers=headers, json=data, params=params)
        
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

async def run_worker():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC,
            durable=True, internal=True, auto_delete=False,
        )

        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        logger.info(f"Subscribed to {EXCHANGE_NAME} with key {ROUTING_KEY}. Waiting for messages.")

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    try:
                        body = json.loads(message.body.decode())
                        event_data = body.get("data", {})
                        operation = event_data.get("operation")
                        doc_id = (event_data.get("doc") or {}).get("id")

                        if not doc_id:
                            logger.warning("Message missing doc.id, skipping")
                            continue
                    
                        if operation != "create":
                            logger.debug(f"Ignoring operation={operation} for {doc_id}")
                            continue

                        # Mark communication status as processing
                        api_request("PATCH", f"/api/communications/{doc_id}", {"status": "processing"})
                        
                        doc = api_request("GET", f"/api/communications/{doc_id}", params={"depth": 1})

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


if __name__ == "__main__":
    asyncio.run(run_worker())