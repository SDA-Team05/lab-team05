# Software Design and Architecture - Group 05 labs implementation

## Lab objectives
The laboratory revolves around decoupling the Email processing function from MZinga. Currently, MZinga has

## Lab 1: DB-Coupled External Worker

The goal for the lab was to introduce a new service, with the responsibility of reading the communications pending processing from the MongoDB database. In this first version, the service reads directly from the shared MongoDB database. 

### MZinga Setup

The first thing that needs to be done is configuring MZinga to allow the usage of an external worker for email processing.

#### Add field to Communications

To allow our worker to understand which communications need to be processed, we must add a "status" fields to the communications.

```javascript
    {
      name: "status",
      type: "select",
      admin: {
        readOnly: true,
        position: "sidebar",
      },
      options: [
        { label: "Pending", value: "pending" },
        { label: "Processing", value: "processing" },
        { label: "Sent", value: "sent" },
        { label: "Failed", value: "failed" },
      ],
    },
```

To show this field in the GUI, we must add it to the "defaultColumns" list in the admin block:

```javascript
    defaultColumns: ["subject", "tos", "status"],
```

And last, we need to modify the afterChange hook in order to support the new field.
We set it so that, if an environment variable we define (**COMMUNICATIONS_EXTERNAL_WORKER**) is set to "true", once a document is created we set the document status to "pending" and we immediately return:
```javascript
    afterChange: [
        async ({ doc, operation }) => {  
        if (process.env.COMMUNICATIONS_EXTERNAL_WORKER === "true") { 
            if (doc.status !== "pending") {
            await payload.update({
                collection: Slugs.Communications,
                id: doc.id,
                data: { status: "pending" },
            });
            }
            return doc; 
        }
        ...
```
If set to false, we keep the old logic and set the document status to "sent":

```javascript
    ...
    await payload.update({
    collection: Slugs.Communications,
    id: doc.id,
    data: { status: "sent" },
    });
```

To avoid endless loops, we add a guard on top of the hook:

```javascript
    if (doc.status === "pending" || doc.status === "sent") {
        return doc;
    }
```

#### Environment Variable

After that, we set our environment variable to true, in order to start testing our new microservice.

```python
COMMUNICATIONS_EXTERNAL_WORKER=true
```

### Microservice Setup

We then create our microservice, and we start by defining some parts that aren't strictly tied to the business logic, but are needed for the proper functioning of the worker.

#### Environment Variables

The first thing we do is setting all of the environment variables needed to authenticate on MongoDB, decide the poll interval and set the SMTP Client.

```python
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/mzinga?authSource=admin&directConnection=true")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
```

#### Logger

To understand what is happening in our worker and debug, we set a logger.

```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
```

#### MongoDB

Before getting to the business logic, we log into MongoDB, and store the columns that we need in variables: *communications* and *users*.

```python
client = MongoClient(MONGODB_URI)
db = client.get_database() 
comms_col = db.communications
users_col = db.users
```

### Utility methods

When developing this first instance of the worker, we developed the first methods that implement the core logic of the microservice, that would be used also in the following versions.

#### serialize_body

In MZinga, the body of a communication is a Slate SAT: a list of node objects with a type and children.
This function converts this structure in an HTML string, translating the supported tags to their HTML equivalent.

```python 
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
```

#### resolve_emails

In MZinga, the email recipients (tos), the Carbon Copies (ccs) and the Blind Carbon Copies (bccs) are not directly present in the communication, but are implemented as a reference to the actual addresses in the *users* collection.
For this reason, we need a function to resolve the ids to actual email addresses.

```python
def resolve_emails(refs: List[Dict]) -> List[str]:
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
```

#### send_email

This function builds a MIMEMultipart message with all of the required information, and sends it using Python's built-in smtplib.

```python 
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
        server.send_message(msg, to_addrs=all_recipients)
```

#### process

The core function of the worker, which:
- Gathers all of the addresses with the *resolve_emails* function
- Converts the body to html with the *serialize_body* function
- Sends the email with the *resolve_emails* function
- Sets the doc status to *sent*

```python 
def process(doc: Dict[str, Any]) -> None:
    doc_id = doc["_id"]
    try:
        logger.info(f"Processing Communication ID: {doc_id}")

        tos = resolve_emails(doc.get("tos", []))
        ccs = resolve_emails(doc.get("ccs", []))
        bccs = resolve_emails(doc.get("bccs", []))

        html_content = serialize_body(doc.get("body", []))

        send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

        comms_col.update_one({"_id": doc["_id"]}, {"$set": {"status": "sent"}})
        logger.info(f"Successfully sent: {doc['_id']}")

    except Exception as e:
        logger.error(f"Error processing {doc['_id']}: {e}")
        comms_col.update_one(
            {"_id": doc["_id"]}, 
            {"$set": {"status": "failed", "error": str(e)}}
        )
```

### Main loop

With all of the previous functions declared, we can write the "main" function, which:
- Logs the startup
- Polls one pending document
- Immediately sets its status to *processing*, to prevent two worker instances from processing the same document.
- Calls the *process* function, which gathers and converts all the necessary data and sends the email
- Sleeps for the configured time before trying to poll again

```python
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
        if doc:
            process(doc)
        else:
            time.sleep(POLL_INTERVAL_SECONDS)
```

## Lab 2: REST API-Coupled External Worker
In the second version of the worker, the database coupling is removed. Instead, it interacts with MZinga exclusively through its auto-generated REST API, authenticated via JWT. The worker is now schema-agnostic.


### Mzinga Setup
In order to enable our microservice to update the document status through REST API, we must explicitly allow it in the access rules. So, we set the update rule to use *access.GetIsAdmin*: this way, Admin users can update documents (our worker will be authenticated as an admin).

```python
  access: {
    read: access.GetIsAdmin,
    create: access.GetIsAdmin,
    delete: () => {
      return false;
    },
    update: access.GetIsAdmin,
  },
```

### Microservice Setup

Now we can start writing our microservice. We start from the previous one and add the credentials for login (created from the application dashboard) and the URL for the API calls.

```python
API_URL = os.getenv("MZINGA_URL")
ADMIN_EMAIL = os.getenv("MZINGA_EMAIL")
ADMIN_PASSWORD = os.getenv("MZINGA_PASSWORD")
```

### Authentication

To perform the API requests that require authentication, we write a method that authenticates the service using the inserted credentials and stores the returned token in the session header.

```python
def authenticate(session: requests.Session) -> None:
    """Authenticates the session and updates its headers with the token."""
    url = f"{API_URL}/api/users/login"
    payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    response = session.post(url, json=payload)
    response.raise_for_status()
    
    token = response.json().get("token")
    session.headers.update({"Authorization": f"Bearer {token}"})
    logger.info("Authenticated")
```
#### api_request

We then write a function that can send any kind of API request. If the authorization token expires, the *authenticate* function is automatically called again.

```python
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
```

### New resolve_email

This time we can't access directly the data in MongoDB, but we still need to resolve the email addresses.
Luckily, in MZingas' endpoint for retrieving communications, if we put *depth=1* as a parameter, the relationships get authomatically resolved.
For this reason, the *resolve_emails* functions now only serves the purpose of extracting the email addresses.

```python
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
```

### New process_document

The new process_document function is mostly identical to the previous one, with the only differences being that it now also handles the setting of the document status to "processing" and that it now uses the API requests for all of the status modifications.

```python
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
```

### Main loop

With all of the functions declared, we can write the "main" function, which:
- Logs the startup
- Performs an initial authentication, and immediately stops if it fails
- Polls the pending documents
- If there are pending documents, calls the *process* function for each one, which gathers and converts all the necessary data and sends the email
- Sleeps for the configured time before trying to poll again

```python
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
```

## Lab 2 - b: Event-Driven Microservice via RabbitMQ

In this optional extension, we move from a REST API based microservice to a more efficient Event based one. This way, we can entirely remove the polling. MZinga supports publishing events to RabbitMQ when a `Communications` document is saved,  requiring only an environment variable change.

### MZinga Setup

The first thing we need to do is change the environment variables so that MZinga can start publishing events on RabbitMQ.

```python
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
HOOKSURL_COMMUNICATIONS_AFTERCHANGE=rabbitmq
```

### Microservice Setup

We need to configure the microservice with all of the necessary data to read events from RabbitMQ

```python
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
ROUTING_KEY = os.environ["ROUTING_KEY"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
```

### Main loop

All of the processing functions remain largely unchanged, since the modifications to the document status still happen through API calls.

The only component that sees some changes to the logic is the Main function, which now:
- Logs the startup
- Performs an initial authentication, and immediately stops if it fails
- Connetts to RabbitMQ
- If an event is published, it makes an API request to fetch the pending documents and calls the *process* function for each one.

```python
async def run_worker():
    with requests.Session() as session:
        try:
            logger.info("Performing initial API authentication...")
            authenticate(session)
        except Exception as e:
            logger.error(f"Fatal: Could not authenticate with MZinga API: {e}")
            return

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
                    async with message.process(requeue=True):
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
                            
                            try:
                                query = f"/api/communications/{doc_id}?depth=1"
                                response_data = api_request(session, "GET", query)

                                doc = response_data

                                if not doc or "id" not in doc:
                                    logger.error(f"Document {doc_id} not found or malformed in response")
                                    continue

                                process_document(session, doc)
                                
                            except Exception as e:
                                logger.error(f"Error processing {doc_id}: {e}")
                                api_request(session, "PATCH", f"/api/communications/{doc_id}", {
                                    "status": "failed", 
                                    "error": str(e)
                                })

                        except requests.HTTPError as e:
                            logger.error(f"HTTP error processing message: {e}")
                            raise
```

## Lab 3: REST API-Coupled External Worker with structured logging and telemetry

Now we want to improve our REST API worker, by adding logs, metrics and traces using OpenTelemetry.

### Context

Adding the three pillars of observability (logs, metrics, and traces) to our microservice enables us to observe what the worker is doing, how long each operation takes, and diagnose failures without reading source code.

#### Logs

**Logs** are timestamped text records of discrete events.
Our worker, so far, uses Python's logging module, which works, but has an important limitation: it is plain text.
This means that it's difficult to query, correlate, and aggregate.
We can fix this by switching to structured logging, which uses JSON objects. This enables us to have basically the same output in the terminal (with minor differences), but we have the possibility of indexing, filtering, and aggregating the entries using a log management system.

#### Metrics

**Metrics** are numeric measurements aggregated over time. 
Adding them to our worker enables us to answer questions like "how many emails were sent in the last minute?" or "what is the 95th percentile processing time?".

#### Traces

A **trace** represents the end-to-end journey of a single unit of work through a system.
A **span** is a single named, timed operation within a trace. 

Each span records:
- A name
- A start time and duration
- A trace ID — shared by all spans in the same trace, used to correlate them
- A span ID — unique to this span
- A parent span ID — the span ID of the parent (absent on the root span)
- Attributes — key-value metadata (e.g. doc_id, recipient_count, http.status_code)
- Events — timestamped annotations within the span (e.g. "SMTP connection established")
- A status — OK or ERROR, with an optional error message

In our worker, one trace corresponds to processing one **Communications** document, and the spans are the various sub-operations (fetch_document, serialize_body, send_email, update_status).

Traces are useful to understand exactly what happend during a failed operation, how long each preceding step took, and what the HTTP response code was from the MZinga API. It is more detailed and easier to read than logs.

### OpenTelemetry setup

### Structured Logging

```python
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
```

### Traces Setup

```python
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
```

### Metrics Setup

```python
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
```

### Updated Methods

#### send_email

```python
def send_email(to_list: List[str], cc_list: List[str], bcc_list: List[str], subject: str, html_body: str) -> None:
    """Sends an email using SMTP. Measures the duration of the send operation and records it in a histogram."""
    with tracer.start_as_current_span("send_email") as span:
        span.set_attribute("recipient_count", len(to_list))
        t0 = time.perf_counter()
        
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(to_list)
        if cc_list: msg["Cc"] = ", ".join(cc_list)
        
        msg.attach(MIMEText(html_body, "html"))
        
        all_recipients = to_list + (cc_list or []) + (bcc_list or [])
        
        if not all_recipients:
            log.warning("No recipients found. Skipping SMTP send.")
            return

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.send_message(msg, to_addrs=all_recipients)
            
        smtp_duration.record(time.perf_counter() - t0)
```

#### process_document

```python
def process_document(session: requests.Session, doc: Dict[str, Any]) -> None:
    """Processes a single communication document: updates status, resolves emails, serializes body, sends email, and updates status again. All steps are traced and logged."""
    doc_id = doc["id"]
    structlog.contextvars.bind_contextvars(doc_id=doc_id)

    with tracer.start_as_current_span("process_communication") as span:
        span.set_attribute("doc_id", doc_id)
        t0 = time.perf_counter()
        log.info("processing_started")

        try:
            api_request(session, "PATCH", f"/api/communications/{doc_id}", {"status": "processing"})

            tos = resolve_emails(doc.get("tos", []))
            ccs = resolve_emails(doc.get("ccs", []))
            bccs = resolve_emails(doc.get("bccs", []))
            
            if not tos:
                raise ValueError("No valid 'to' email addresses found")

            with tracer.start_as_current_span("serialize_body") as s:
                body_nodes = doc.get("body", [])
                s.set_attribute("node_count", len(body_nodes))
                html_content = serialize_body(body_nodes)

            send_email(tos, ccs, bccs, doc.get("subject", "(No Subject)"), html_content)

            api_request(session, "PATCH", f"/api/communications/{doc_id}", {"status": "sent"})
            
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
                api_request(session, "PATCH", f"/api/communications/{doc_id}", {
                    "status": "failed", 
                    "error": str(e)
                })
            except Exception as patch_err:
                log.error("failed_to_update_status", error=str(patch_err))

    structlog.contextvars.unbind_contextvars("doc_id")
```


#### Main loop

```python
def run_worker() -> None:
    log.info("worker_started", poll_interval_s=POLL_INTERVAL_SECONDS, prometheus_port=PROMETHEUS_PORT)
    
    with requests.Session() as session:
        try:
            log.info("performing_initial_authentication")
            authenticate(session)
        except Exception as e:
            log.error("failed_to_authenticate", error=str(e))
            return
        while True:
            try:
                query = "/api/communications?where[status][equals]=pending&sort=createdAt&depth=1"
                response_data = api_request(session, "GET", query)
                docs = response_data.get("docs", [])
                
                if not docs:
                    poll_counter.add(1, {"result": "empty"})
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                    
                poll_counter.add(1, {"result": "found", "count": len(docs)})

                for doc in docs:
                    process_document(session, doc)

            except Exception as e:
                log.error("error_during_polling_loop", error=str(e))
                time.sleep(POLL_INTERVAL_SECONDS)
```