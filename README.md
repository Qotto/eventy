# eventy

Handy module for designing event-driven systems


## Overview

Eventy defines components to help building an event-drivven app.

* event / command model
* serializer of event / command
* consumer of event / command
* emitter of event / command
* consumer of http requests
* app used as the glue for all this components

The module provides some implementations :

* avro serializer
* kafka consumer / emitter
* Sanic http request handler

## Usage

server.py file :

```python
from eventy.app.eventy import Eventy
...

# init eventy app
app = Eventy()

# configure the serializer
# requires EVENTY_EVENT_SERIALIZER config variable to be set
# if Avro serializer is used, requires additional AVRO_SCHEMAS_FOLDER variable to be set
serializer = app.configure_event_serializer(
    settings=settings, serializer_name='serializer')

# register event/command classes using the name defined in avro schema
serializer.register_event_class(CreatePaymentCommand)
serializer.register_event_class(PaymentCreatedEvent)

# configure a command consummer
# requires EVENTY_EVENT_CONSUMER config variable to be set
# if Kafka consumer is used, requires additional KAFKA_BOOTSTRAP_SERVER config variable
app.configure_consumer(settings=settings, serializer=serializer, consumer_name='payment_commands_consummer',
                       event_topics=['payment-commands'], event_group='payment', position='latest')

# configure an event consummer
# requires EVENTY_EVENT_CONSUMER config variable to be set
# if Kafka consumer is used, requires additional KAFKA_BOOTSTRAP_SERVER config variable
app.configure_consumer(settings=settings, serializer=serializer, consumer_name='payment_events_consummer',
                       event_topics=['payment-events'], event_group=None, position='earliest')

# configure a command/event emitter named emitter
# requires EVENTY_EVENT_EMITTER config variable to be set
# if Kafka producer is used, requires additional KAFKA_BOOTSTRAP_SERVER config variable
app.configure_emitter(settings=settings, serializer=serializer,
                      emitter_name="emitter")

# configure the http handler
# requires EVENTY_HTTP_HANDLER_PORT config variable to be set
app.configure_http_handler(
    settings=settings, http_handler_name='http_handler')

# register a blueprint on the http handler (sanic)
app.get('http_handler').blueprint(sms.bp)

# register some elements in the app
app.set('payment_repository', adapter.get_payment_repository())


# register other async tasks
asyncio.ensure_future(myTask.run())

# start the app - actually launch the asyncio loop
app.start()

```

settings.py :

```python

# Eventy configuration
EVENTY_EVENT_SERIALIZER = 'eventy.serializer.avro.AvroEventSerializer'
EVENTY_EVENT_CONSUMER = 'eventy.consumer.kafka.KafkaConsumer'
EVENTY_EVENT_EMITTER = 'eventy.emitter.kafka.KafkaProducer'
EVENTY_HTTP_HANDLER_PORT = 8000

# Avro configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AVRO_SCHEMAS_FOLDER = os.path.join(BASE_DIR, 'avro_schemas')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVER = 'qottodev:31090'

```

commands.py :

```python
from eventy.command.base import BaseCommand

class CreatePaymentCommand(BaseCommand):

    def __init__(self, data: Dict[str, Any]) -> None:
        super().__init__(data=data)

    # This method is called by the consumer to instantiate the class
    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        ...

    # Map the name with the event defined in the avro schema
    # This method is called when registering the event on the serializer
    @classmethod
    def name(cls):
        return "qotto.payment.command.CreatePayment"

    # This method is called when the command is received
    async def execute(self, app: BaseApp, corr_id: str):

        # Emit an event on topic payment-events
        payment_created_event = PaymentCreatedEvent(data={...})
        await app.get('emitter').send(payment_created_event, 'payment-events')

        # Previously registered elements can be retrieved using their name
        payment_repository = app.get('payment_repository')
        ...
```

events.py :

```python
from eventy.event.base import BaseEvent

class PaymentCreatedEvent(BaseEvent):

    def __init__(self, data: Dict[str, Any]) -> None:
        super().__init__(data=data)


    # This method is called by the consumer to instantiate the class
    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        ...

    @classmethod
    def name(cls):
        return 'qotto.payment.event.PaymentEvent'


    # This method is called when the event is received
    async def handle(self, app: BaseApp, corr_id: str):
        ...
```

blueprints.py :

```python
from sanic import Blueprint

bp = Blueprint('sms')

@bp.route('/sms', methods=['POST'])
async def sms(request):
    # app is registered on each request
    app = request['app']
    ...

```