import json
import pika
import django
import os
import sys
from django.core.mail import send_mail
import time
from pika.exceptions import AMQPConnectionError

sys.path.append("")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "presentation_mailer.settings")
django.setup()

while True:
    try:

        def process_message(ch, method, properties, body):
            content = json.loads(body)
            send_mail(
                'Your presentation has been accepted',
                content["presenter_name"] + ", " + "we're happy to tell you that your presentation " + content["title"] + ' has been accepted',
                'admin@example.com',
                [content['presenter_email']],
                fail_silently=False,
            )
            print("Sent approval email")

        def process_message_rejection(ch, method, properties, body):
            content = json.loads(body)
            send_mail(
                'Your presentation has been rejected',
                content["presenter_name"] + ", " + "we're sad to tell you that your presentation " + content["title"] + ' has been rejected',
                'admin@example.com',
                [content['presenter_email']],
                fail_silently=False,
            )
            print("Sent rejection email")

        parameters = pika.ConnectionParameters(host='rabbitmq')
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        ##APPROVALS##

        # Create a queue if it does not exist
        channel.queue_declare(queue='presentation_approvals')
        # Configure the consumer to call the process_message function
        # when a message arrives
        channel.basic_consume(
            queue='presentation_approvals',
            on_message_callback=process_message,
            auto_ack=True,
        )

        ##REJECTIONS##

        # Create a queue if it does not exist
        channel.queue_declare(queue='presentation_rejections')
        # Configure the consumer to call the process_message function
        # when a message arrives
        channel.basic_consume(
            queue='presentation_rejections',
            on_message_callback=process_message_rejection,
            auto_ack=True,
        )
        # Tell RabbitMQ that you're ready to receive messages
        channel.start_consuming()

        ###################
    except AMQPConnectionError:
        print("Could not connect to RabbitMQ")
        time.sleep(2.0)
