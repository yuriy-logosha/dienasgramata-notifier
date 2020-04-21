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

COMMASPACE = ', '
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
            return {"data": el[0]['date'].strftime("%d %B %Y"),
                    'tema': el[0]['tema'] if len(el[0]['tema'].strip()) > 0 else "-",
                    'uzdots': el[0]['exercise'] if len(el[0]['exercise'].strip()) > 0 else "-",
                    'subject': el[0]['subject'] if len(el[0]['subject'].strip()) > 0 else "-"}

        def build_html_body(mapped_msg):
            return f"<table>" \
                f"<tr>" \
                f"<td>Data:</td><td>{mapped_msg['data']}</td>" \
                f"</tr>" \
                f"<tr>" \
                f"<td>Priekšmets:</td><td>{mapped_msg['subject']}</td>" \
                f"</tr>" \
                f"<tr>" \
                f"<td>Tēma:</td><td>{mapped_msg['tema']}</td>" \
                f"</tr>" \
                f"<tr>" \
                f"<td>Uzdots:</td><td>{mapped_msg['uzdots']}</td>" \
                f"</tr>" \
                f"</table>"

        def build_text_body(mapped_msg):
            return f"Data: {mapped_msg['data']}" \
                f"\r\n" \
                f"Priekšmets: {mapped_msg['subject']}" \
                f"\r\n" \
                f"Tēma: {mapped_msg['tema']}" \
                f"\r\n" \
                f"Uzdots: {mapped_msg['uzdots']}"

        def build_subject(txt):
            return Header(f"{txt}", UTF_8)

        def build_html_envelope(html, txt):
            style = "table {border-spacing: 5px;margin: 20px 0;}"

            # "<span class=3D"preheader" style=3D"-webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; max-width: 0; color: transparent; height: 0; max-height: 0; width: 0; opacity: 0; overflow: hidden; mso-hide: all; visibility: hidden; display: none !important;">It's not too late for you, your friends, and your family members to get a free one-year subscription to Balance, our new meditation app. </span>""
            # Content-Transfer-Encoding: quoted-printable

            body = "<!DOCTYPE html><html lang=3D\"en\" xmlns=3D\"http://www.w3.org/1999/xhtml\" xmlns:v=3D\"urn:schemas-microsoft-com:vml\" xmlns:o=3D\"urn:schemas-microsoft-com:office:office\"style=3D\"-ms-text-size-adjust: 100%; -webkit-text-size-adjust: 100%; height: 100% !important; width: 100% !important; margin: 0; padding: 0;\"><head style=3D\"-ms-text-size-adjust: 100%; -webkit-text-size-adjust: 100%;\"><meta http-equiv=\"Content-Type\" content=\"text/html; charset=3DUTF-8\"/><style>" + style +"</style></head><body><div class=3D\"preheader\" style=3D\"display:none!important;font-size:1px;color:#333333;line-height:1px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;mso-hide:all\">" + txt +"</div><div>"+ html + "</div></body></html>"
            msg = MIMEText(body, 'html', UTF_8)
            return msg

        def build_text_envelope(txt):
            msg = MIMEText(txt, 'plain', UTF_8)
            return msg

        def build_envelope(env, subj):
            env["From"] = config['email.from']
            env["Subject"] = subj
            env["To"] = COMMASPACE.join(get_emailers())
            return env.as_string()

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
                    infos_html = []
                    infos_text = []
                    subject = []

                    if 'inserted' in message.value:
                        group = config['email.subj.inserted']
                        infos_html.append(f"<h2>{group}</h2>")
                        infos_text.append(f"{group}\r\n")
                        for id in message.value['inserted']:
                            info = get_dienasgramata_info(id)
                            infos_html.append(build_html_body(info))
                            infos_text.append(build_text_body(info)+"\r\n")
                        subject.append(group)

                    if 'updated' in message.value:
                        group = config['email.subj.updated']
                        infos_html.append(f"<h2>{group}</h2>")
                        infos_text.append(f"{group}\r\n")
                        for id in message.value['updated']:
                            info = get_dienasgramata_info(id)
                            infos_html.append(build_html_body(info))
                            infos_text.append(build_text_body(info)+"\r\n")
                        subject.append(group)

                    subj = build_subject("".join(subject) if len(subject) == 1 else " & ".join(subject))
                    send_email(build_envelope(build_text_envelope("\r\n".join(infos_text)), subj))
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