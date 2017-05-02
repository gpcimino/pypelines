import smtplib
from email.mime.text import MIMEText
import json
import logging
import traceback

from ..internals.dag import DAGNode


class SMTP(DAGNode):
    def __init__(self, dest_addrs, server_addr, from_address, subject, server_port=25):
        super().__init__()        
        self._addresses = dest_addrs
        self._server_ip = server_addr
        self._server_port = server_port
        self._from_address = from_address
        self._subject = subject

    def on_data(self, data):
        self.send_mail(str(data))

    def get_server_address(self):
        return self._addresses

    def send_mail(self, body):
        log = logging.getLogger(__name__)

        if len(self._addresses) == 0:
            log.info("Cannot find any destination email address: nothing to do.")
            return

        log.debug("Found dest email: " + str(self._addresses) + "; len=" + str(len(self._addresses)))

        for addr in self.get_server_address():
            all_mail_addr = [addr]
            try:
                log.info("Start sending emails...")
                log.info("mail server ip:  " + str(self._server_ip))
                log.info("mail server tcp port: " + str(self._server_port))

                server = smtplib.SMTP(self._server_ip, port=self._server_port, timeout=100)
                #server.login("assetplanning", "klajsd873jd&^shF")

                msg = MIMEText(body)
                msg['Subject'] = self._subject
                msg['From'] = self._from_address
                msg['To'] = ", ".join(all_mail_addr)
                server.set_debuglevel(0)
                log.info("send emails from: " + str(self._from_address))
                log.info("send emails to single receiver: " + str(all_mail_addr))
                log.info("subject: " + str(self._subject))
                log.info("body: " + str(body))
                server.sendmail(self._from_address, all_mail_addr, msg.as_string())
                server.quit()
                log.info("send mail completed without errors")
            except Exception as ex:
                log.exception("Failure in mail sending to " + ", ".join(all_mail_addr))
