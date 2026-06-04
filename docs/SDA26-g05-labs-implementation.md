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
        if (process.env.COMMUNICATIONS_EXTERNAL_WORKER === "true" && operation === "create") { 
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

#### Environment Variable

After that, we set our environment variable to true, in order to start testing our new microservice.

```python
COMMUNICATIONS_EXTERNAL_WORKER=true
```

### Microservice Setup

#### Environment Variables
```python
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/mzinga?authSource=admin&directConnection=true")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
```
#### Logger
```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
```

#### MongoDB
```python
client = MongoClient(MONGODB_URI)
db = client.get_database() 
comms_col = db.communications
users_col = db.users
```

### Utility methods

When developing this first instance of the worker, we developed the first methods that implement the core logic of the microservice, that would be used also in the following versions.

#### serialize_body
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

### Complete Worker Flow
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
The database coupling is removed. The external worker interacts with MZinga exclusively through its auto-generated REST API, authenticated via JWT. The Remote Facade pattern governs the integration. The worker is now schema-agnostic.

### Setup
```python
API_URL = os.getenv("MZINGA_URL")
ADMIN_EMAIL = os.getenv("MZINGA_EMAIL")
ADMIN_PASSWORD = os.getenv("MZINGA_PASSWORD")
```

### Authentication
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

### email addresses Resolution
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

### Complete Worker Flow
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
The polling is removed. The monolith publishes an event to RabbitMQ when a `Communications` document is saved — requiring only an environment variable change, no code modification. The worker subscribes and reacts. The Publish/Subscribe pattern governs the integration. The monolith and the worker are fully decoupled.

### Setup

#### MZinga Configuration
```python
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
HOOKSURL_COMMUNICATIONS_AFTERCHANGE=rabbitmq
```
#### Environment Variables

```python
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
ROUTING_KEY = os.environ["ROUTING_KEY"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
```

### Complete Worker Flow
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

### Setup

#### OpenTelemetry