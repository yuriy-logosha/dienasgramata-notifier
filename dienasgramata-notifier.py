#!/usr/bin/env python3

import logging
import locale
import time
import json
from kafka import KafkaConsumer
import pymongo
from bson import ObjectId
from email.mime.text import MIMEText
from email.header import Header
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

api_version = (config["kafka.api.version.major"], config["kafka.api.version.middle"], config["kafka.api.version.minor"])
UTF_8 = 'utf-8'
for loc in config["locale"]:
    try:
        locale.setlocale(locale.LC_ALL, loc)
        break
    except locale.Error:
        continue

while True:
    myclient = pymongo.MongoClient(config["db.url"])

    with myclient:
        dienasgramata = myclient.school.dienasgramata


        def get_emailers():
            emails = []
            for e in list(myclient.school.notification.find({"enabled": True})):
                emails.append(f"{e['name']} <{e['email']}>")
            return emails

        def map(el):
            return {"data":el[0]['date'].strftime("%d %B %Y"), 'tema': el[0]['tema'], 'uzdots':el[0]['exercise'], 'subject':el[0]['subject']}

        def build_body(mapped_msg):
            return f"Data: {mapped_msg['data']}\r\nPriekšmets: {mapped_msg['subject']}\r\nTēma: {mapped_msg['tema']}\r\nUzdots: {mapped_msg['uzdots']}"

        def build_subject(txt):
            return Header(f"{txt}", UTF_8)

        def build_envelope(bodys, subj) -> str:
            msg = MIMEText("\r\n\r\n".join(bodys), 'plain', UTF_8)
            msg["From"] = config['email.from']
            msg["Subject"] = subj
            msg["To"] = ", ".join(get_emailers())
            return msg.as_string()

        def send_email(msg):
            print("{}".format(msg))
            p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE, universal_newlines=True)
            p.communicate(msg)

        def get_dienasgramata_info(id):
            el = dienasgramata.find({"_id": ObjectId(id)})
            if el:
                return map(el)

        try:
            consumer = KafkaConsumer(config['kafka.topic'], bootstrap_servers=[config['kafka.host']], group_id=config['kafka.group'], enable_auto_commit=False, api_version=api_version, value_deserializer=lambda x: json.loads(x.decode(UTF_8)))

            for message in consumer:
                try:
                    infos = []
                    subject = []
                    if 'inserted' in message.value:
                        infos.append("\r\n\r\n".join([build_body(get_dienasgramata_info(id)) for id in message.value['inserted']]))
                        subject.append(config['email.subj.inserted'])
                    if 'updated' in message.value:
                        infos.append("\r\n\r\n".join([build_body(get_dienasgramata_info(id)) for id in message.value['updated']]))
                        subject.append(config['email.subj.updated'])
                    send_email(build_envelope(infos, build_subject("".join(subject) if len(subject) == 1 else " & ".join(subject))))
                    consumer.commit()
                except Exception as e:
                    logger.exception(e)

        except RuntimeError as e:
            logger.error(e)
            db_records = []

        if 'restart' in config and config['restart'] > 0:
            logger.info("Waiting %s seconds.", config['restart'])
            time.sleep(config['restart'])
        else:
            break