#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jinja2
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPAuthenticationError
from notification import BaseNotification

import logging
logger = logging.getLogger(__name__)

thisdir = os.path.join(os.path.dirname(__file__))

template_dir = os.path.join(thisdir, '../templates/mail')
jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir),
                               trim_blocks=True,
                               lstrip_blocks=True)


class Mail(BaseNotification):

    @classmethod
    def notify(cls, alert, *args, **kwargs):

        alert_def = alert['alert_def']
        logger.info("Sending email for alert: {}".format(alert_def['id']))

        sender = cls._config.get('notifications.mail.sender')
        subject = cls._get_subject(alert, custom_message=kwargs.get('subject'))
        html = kwargs.get('html', False)
        cc = kwargs.get('cc', [])
        hide_recipients = kwargs.get('hide_recipients', True)
        repeat = kwargs.get('repeat', 0)
        include_value = kwargs.get('include_value', True)
        include_definition = kwargs.get('include_definition', True)
        include_captures = kwargs.get('include_captures', True)
        include_entity = kwargs.get('include_entity', True)
        expanded_alert_name = cls._get_expanded_alert_name(alert)

        try:
            tmpl = jinja_env.get_template('alert.txt')
            body_plain = tmpl.render(expanded_alert_name=expanded_alert_name,
                                     include_value=include_value,
                                     include_definition=include_definition,
                                     include_captures=include_captures,
                                     include_entity=include_entity,
                                     **alert)
        except Exception:
            logger.exception('Error parsing email template for alert %s with id %s', alert_def['name'], alert_def['id'])
        else:
            if html:
                msg = MIMEMultipart('alternative')
                tmpl = jinja_env.get_template('alert.html')
                body_html = tmpl.render(expanded_alert_name=expanded_alert_name,
                                        include_value=include_value,
                                        include_definition=include_definition,
                                        include_captures=include_captures,
                                        include_entity=include_entity,
                                        **alert)
                part1 = MIMEText(body_plain.encode('utf-8'), 'plain', 'utf-8')
                part2 = MIMEText(body_html.encode('utf-8'), 'html', 'utf-8')
                msg.attach(part1)
                msg.attach(part2)
            else:
                msg = MIMEText(body_plain.encode('utf-8'), 'plain', 'utf-8')

            msg['Subject'] = subject
            msg['From'] = 'ZMON 2 <{}>'.format(sender)

            args = BaseNotification.resolve_group(args)

            if hide_recipients:
                msg['To'] = 'Undisclosed Recipients <{}>'.format(sender)
                msg['Bcc'] = ', '.join(args)
            else:
                msg['To'] = ', '.join(args)
            msg['Cc'] = ', '.join(cc)

            mail_host = cls._config.get('notifications.mail.host', 'localhost')
            mail_port = cls._config.get('notifications.mail.port', '25')

            # logger.info("Relaying via %s %s", mail_host, mail_port)

            if cls._config.get('notifications.mail.on', True):
                try:
                    if mail_host != 'localhost':
                        s = smtplib.SMTP_SSL(mail_host, mail_port)
                    else:
                        s = smtplib.SMTP(mail_host, mail_port)

                except Exception:
                    logger.exception('Error connecting to SMTP server %s for alert %s with id %s',
                                     mail_host, alert_def['name'], alert_def['id'])
                else:
                    try:
                        mail_user = cls._config.get('notifications.mail.user', None)
                        if mail_user is not None:
                            s.login(mail_user, cls._config.get('notifications.mail.password'))

                        s.sendmail(sender, list(args) + cc, msg.as_string())
                    except SMTPAuthenticationError:
                        logger.exception(
                            'Error sending email for alert %s with id %s: authentication failed for %s',
                            alert_def['name'], alert_def['id'], mail_user)
                    except Exception:
                        logger.exception(
                            'Error sending email for alert %s with id %s', alert_def['name'], alert_def['id'])
                    finally:
                        s.quit()
        finally:
            return repeat


if __name__ == '__main__':
    import sys
    Mail.notify({'entity': {'id': 'test'}, 'value': 5}, *sys.argv[1:])
