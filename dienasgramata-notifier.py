#!/usr/bin/env python3

import logging
import time
import json
from kafka import KafkaConsumer
import pymongo
from bson import ObjectId
from email.mime.text import MIMEText
from subprocess import Popen, PIPE

from utils import json_from_file

config_file_name = 'config.json'
config = {}

try:
    config = json_from_file(config_file_name, "Can't open ss-config file.")
except RuntimeError as e:
    print(e)
    exit()

formatter = logging.Formatter(config['logging.format'])
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(config['logging.file'])

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging_level = config["logging.level"] if 'logging.level' in config else 20
print("Selecting logging level", logging_level)
print("Selecting logging format", config["logging.format"])
print("Selecting logging file \"%s\"" % config['logging.file'])

logging.basicConfig(format=config["logging.format"], handlers=[c_handler, f_handler])
logger = logging.getLogger(config["logging.name"])
logger.setLevel(logging_level)


while True:
    myclient = pymongo.MongoClient(config["db.url"])

    with myclient:
        dienasgramata = myclient.school.dienasgramata

        def email(id, tag):
            el = list(dienasgramata.find({"_id": ObjectId(id)}))
            if el and el[0]:
                msg = MIMEText("Data: {}\r\nTēma: {}\r\nUzdots: {}".format(el[0]['date'].strftime("%d %B %Y"), el[0]['tema'], el[0]['exercise']))
                msg["From"] = config['email.from']
                msg["Subject"] = "{} {}".format(config['email.subj.' + tag], el[0]['subject'])
                msg["To"] = ", ".join(config['email.to'])
                print("{}".format(msg))
                p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE, universal_newlines=True)
                p.communicate(msg.as_string())

        try:
            consumer = KafkaConsumer(config['kafka.topic'], bootstrap_servers=[config['kafka.host']], group_id=config['kafka.group'], enable_auto_commit=False, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

            for message in consumer:
                try:
                    if 'inserted' in message.value:
                        for d in message.value['inserted']:
                            email(d, 'inserted')
                    if 'updated' in message.value:
                        for d in message.value['updated']:
                            email(d, 'updated')
                except Exception as e:
                    logger.error(e)
                consumer.commit()

        except RuntimeError as e:
            logger.error(e)
            db_records = []

        if 'restart' in config and config['restart'] > 0:
            logger.info("Waiting %s seconds.", config['restart'])
            time.sleep(config['restart'])
        else:
            break



