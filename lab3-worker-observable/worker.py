import os
import time
import logging
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import requests
import structlog

# OpenTelemetry — tracing
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# OpenTelemetry — metrics
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader  # <-- Updated import
from prometheus_client import start_http_server

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = os.environ["MZINGA_API_URL"]
ADMIN_EMAIL = os.environ["ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
SERVICE_NAME_VALUE = os.getenv("OTEL_SERVICE_NAME", "email-worker")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8000))

# OpenTelemetry - Tracing

resource = Resource(attributes={
    SERVICE_NAME: SERVICE_NAME_VALUE,
    SERVICE_VERSION: "1.0.0",
})

tracer_provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces")
tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(tracer_provider)

RequestsInstrumentor().instrument()

tracer = trace.get_tracer(SERVICE_NAME_VALUE)

# OpenTelemetry - Metrics

start_http_server(port=PROMETHEUS_PORT)
metric_reader = PrometheusMetricReader()
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter(SERVICE_NAME_VALUE)

emails_processed = meter.create_counter(
    name="emails_processed_total",
    description="Total number of communications processed",
    unit="1",
)
processing_duration = meter.create_histogram(
    name="email_processing_duration_seconds",
    description="End-to-end duration of processing one communication",
    unit="s",
)
smtp_duration = meter.create_histogram(
    name="smtp_send_duration_seconds",
    description="Duration of the SMTP send call",
    unit="s",
)
poll_counter = meter.create_counter(
    name="worker_poll_total",
    description="Number of poll cycles",
    unit="1",
)

def add_otel_context(logger, method, event_dict):
    """Inject active trace_id and span_id into every log entry."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        add_otel_context,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger(service=SERVICE_NAME_VALUE)

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
        log.warning("No recipients found. Skipping SMTP send.")
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.send_message(msg, to_addrs=all_recipients)

def run_worker():
    log.info(f"Worker connected to API. Polling every {POLL_INTERVAL_SECONDS}s...")
    
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
            structlog.contextvars.bind_contextvars(doc_id=doc_id)

            try:
                log.info(f"Processing Communication ID: {doc_id}")

                # Mark communication status as processing
                api_request("PATCH", f"/api/communications/{doc_id}", {"status": "processing"})

                tos = resolve_emails(doc.get("tos", []))
                ccs = resolve_emails(doc.get("ccs", []))
                bccs = resolve_emails(doc.get("bccs", []))

                html_content = slate_to_html(doc.get("body", []))

                send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

                api_request("PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
                log.info(f"Successfully sent: {doc_id}")

            except Exception as e:
                log.error(f"Error processing {doc_id}: {e}")
                api_request("PATCH", f"/api/communications/{doc_id}", {
                    "status": "failed", 
                    "error": str(e)
                })

        except Exception as e:
            log.error(f"Error during polling loop: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)
            
        structlog.contextvars.unbind_contextvars("doc_id")

if __name__ == "__main__":
    run_worker()