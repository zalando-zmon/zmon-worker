#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import smtplib
import logging
import jinja2

from urllib2 import urlparse

from opentracing_utils import trace, extract_span_from_kwargs

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPAuthenticationError


from zmon_worker_monitor.zmon_worker.errors import NotificationError
from notification import BaseNotification


logger = logging.getLogger(__name__)

thisdir = os.path.join(os.path.dirname(__file__))

template_dir = os.path.join(thisdir, '../templates/mail')
jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir),
                               trim_blocks=True,
                               lstrip_blocks=True)


class Mail(BaseNotification):

    @classmethod
    @trace(operation_name='notification_mail', pass_span=True, tags={'notification': 'mail'})
    def notify(cls, alert, *args, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        repeat = kwargs.get('repeat', 0)
        alert_def = alert['alert_def']
        per_entity = kwargs.get('per_entity', True)

        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity', {})
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity.get('id'))
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        if not cls._config.get('notifications.mail.on', True):
            current_span.set_tag('mail_enabled', False)
            logger.info('Not sending email for alert: {}. Mail notification is not enabled.'.format(alert_def['id']))
            return repeat

        if not is_changed and not per_entity:
            return repeat

        sender = cls._config.get('notifications.mail.sender')
        subject = cls._get_subject(alert, custom_message=kwargs.get('subject'))
        html = kwargs.get('html', False)
        cc = kwargs.get('cc', [])
        hide_recipients = kwargs.get('hide_recipients', True)
        include_value = kwargs.get('include_value', True)
        include_definition = kwargs.get('include_definition', True)
        include_captures = kwargs.get('include_captures', True)
        include_entity = kwargs.get('include_entity', True)
        expanded_alert_name = cls._get_expanded_alert_name(alert)

        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))
        alert_url = urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_def['id'])) if zmon_host else ''

        try:
            tmpl = jinja_env.get_template('alert.txt')
            body_plain = tmpl.render(expanded_alert_name=expanded_alert_name,
                                     include_value=include_value,
                                     include_definition=include_definition,
                                     include_captures=include_captures,
                                     include_entity=include_entity,
                                     alert_url=alert_url,
                                     **alert)
        except Exception as e:
            current_span.set_tag('error', True)
            current_span.log_kv({'exception': str(e)})
            logger.exception('Error parsing email template for alert %s with id %s', alert_def['name'], alert_def['id'])
        else:
            if html:
                current_span.set_tag('html', True)
                msg = MIMEMultipart('alternative')
                tmpl = jinja_env.get_template('alert.html')
                body_html = tmpl.render(expanded_alert_name=expanded_alert_name,
                                        include_value=include_value,
                                        include_definition=include_definition,
                                        include_captures=include_captures,
                                        include_entity=include_entity,
                                        alert_url=alert_url,
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

            try:
                if mail_host != 'localhost':
                    if cls._config.get('notifications.mail.tls', False):

                        logger.info('Mail notification using TLS!')
                        current_span.set_tag('tls', True)

                        s = smtplib.SMTP(mail_host, mail_port)
                        s.ehlo()
                        if not s.has_extn('STARTTLS'):
                            raise NotificationError('Mail server ({}) does not support TLS!'.format(mail_host))
                        s.starttls()
                        s.ehlo()
                    else:
                        current_span.set_tag('tls', False)
                        s = smtplib.SMTP_SSL(mail_host, mail_port)
                else:
                    s = smtplib.SMTP(mail_host, mail_port)

            except Exception:
                current_span.set_tag('error', True)
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
                except Exception as e:
                    current_span.set_tag('error', True)
                    current_span.log_kv({'exception': str(e)})
                    logger.exception(
                        'Error sending email for alert %s with id %s', alert_def['name'], alert_def['id'])
                finally:
                    s.quit()
        finally:
            return repeat


if __name__ == '__main__':
    import sys
    Mail.notify({'entity': {'id': 'test'}, 'value': 5}, *sys.argv[1:])
