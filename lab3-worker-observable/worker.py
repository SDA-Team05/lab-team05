import os
import time
import logging
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
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
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

load_dotenv()

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
    url = f"{API_URL}/api/users/login"
    payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json().get("token")

def api_request(method, endpoint, data=None):
    global current_token
    if not current_token:
        current_token = get_auth_token()
    
    url = f"{API_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {current_token}"}
    
    response = requests.request(method, url, headers=headers, json=data)
    
    if response.status_code == 401:
        log.info("token_expired_reauthenticating")
        current_token = get_auth_token()
        headers["Authorization"] = f"Bearer {current_token}"
        response = requests.request(method, url, headers=headers, json=data)
        
    response.raise_for_status()
    return response.json()

def serialize_body(nodes):
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
        children_html = serialize_body(node.get("children", []))

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
    if not refs or not isinstance(refs, list):
        return []
    emails = []
    for ref in refs:
        if isinstance(ref, dict) and ref.get("relationTo") == "users":
            user_data = ref.get("value")
            if isinstance(user_data, dict) and "email" in user_data:
                emails.append(user_data["email"])
    return emails

def send_email(to_list, cc_list, bcc_list, subject, html_body):
    with tracer.start_as_current_span("send_email") as span:
        span.set_attribute("recipient_count", len(to_list))
        t0 = time.perf_counter()
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(to_list)
        if cc_list:
            msg["Cc"] = ", ".join(cc_list)
        
        msg.attach(MIMEText(html_body, "html"))
        all_recipients = to_list + (cc_list or []) + (bcc_list or [])
        
        if not all_recipients:
            log.warning("no_recipients_found_skipping")
            return

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.sendmail(EMAIL_FROM, all_recipients, msg.as_string())
        smtp_duration.record(time.perf_counter() - t0)

def run_worker():
    log.info("worker_started", poll_interval_s=POLL_INTERVAL_SECONDS, prometheus_port=PROMETHEUS_PORT)
    
    while True:
        try:
            query = "/api/communications?where[status][equals]=pending&depth=1"
            response = api_request("GET", query)
            docs = response.get("docs", [])
            
            if not docs:
                poll_counter.add(1, {"result": "empty"})
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
                
            poll_counter.add(1, {"result": "found"})
            doc = docs[0]
            doc_id = doc["id"]

            structlog.contextvars.bind_contextvars(doc_id=doc_id)

            with tracer.start_as_current_span("process_communication") as span:
                span.set_attribute("doc_id", doc_id)
                t0 = time.perf_counter()
                log.info("processing_started")

                try:
                    api_request("PATCH", f"/api/communications/{doc_id}", {"status": "processing"})

                    tos = resolve_emails(doc.get("tos", []))
                    ccs = resolve_emails(doc.get("ccs", []))
                    bccs = resolve_emails(doc.get("bccs", []))

                    with tracer.start_as_current_span("serialize_body") as s:
                        body_nodes = doc.get("body", [])
                        s.set_attribute("node_count", len(body_nodes))
                        html_content = serialize_body(body_nodes)

                    send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

                    api_request("PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
                    
                    duration = time.perf_counter() - t0
                    processing_duration.record(duration)
                    emails_processed.add(1, {"status": "sent", "recipient_count": len(tos)})
                    log.info("processing_completed", status="sent", duration_s=round(duration, 3))

                except Exception as e:
                    span.set_status(trace.StatusCode.ERROR, str(e))
                    span.record_exception(e)
                    log.error("processing_failed", error=str(e))
                    
                    emails_processed.add(1, {"status": "failed", "recipient_count": 0})
                    
                    try:
                        api_request("PATCH", f"/api/communications/{doc_id}", {
                            "status": "failed", 
                            "error": str(e)
                        })
                    except Exception as patch_err:
                        log.error("failed_to_update_status", error=str(patch_err))

            structlog.contextvars.unbind_contextvars("doc_id")

        except Exception as e:
            log.error("error_during_polling_loop", error=str(e))
            time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_worker()