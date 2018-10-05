# -*- coding: utf-8 -*-

import os
from dotenv import load_dotenv
import random
import time
from flask import (
    Flask,
    request,
    render_template,
    session,
    flash,
    redirect,
    url_for,
    jsonify,
)
from flask_mail import Mail, Message
from celery import Celery


basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')

# Flask-Mail configuration
app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER')
app.config['MAIL_PORT'] = os.environ.get('MAIL_PORT')
app.config['MAIL_USE_SSL'] = os.environ.get('MAIL_USE_SSL') is not None
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_DEFAULT_SENDER')


# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

# celery v4 defaults to use json as serializer.
# To enable flask-mail, just change the serializer to 'pickle' (default in celery v3)
app.config['CELERY_TASK_SERIALIZER'] = 'pickle'
#app.config['CELERY_RESULT_SERIALIZER'] = 'json'
app.config['CELERY_ACCEPT_CONTENT'] = ['json', 'pickle']


# Init extensions
mail = Mail(app)

# Init Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@celery.task
def send_async_email(msg):
    """Background task to send an email with Flask-Mail"""
    with app.app_context():
        mail.send(msg)


@celery.task(bind=True)
def long_task(self):
    """Background task that returns a long function with progress reports."""
    
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']

    message = ''
    total = random.randint(10, 30)
    
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(
                random.choice(verb),
                random.choice(adjective),
                random.choice(noun))
        
        # custom state: 'PROGRESS'. 
        # built-in states: PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED _    
        self.update_state(state='PROGRESS', meta={
            'current': i,
            'total': total,
            'status': message})
    
        time.sleep(1)

    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}



"""
This example was written for Celery 3, which uses pickle as a default serialization format. 
In Celery 4, the default was changed to JSON, so the error here is that there is no way to serialize the Message object in JSON format.

You can fix the problem in a few ways:

a. downgrade to Celery v3
b. configure your Celery v4 to use pickle as serialization format
c. don't construct a Message instance, just send all the components of the message in a dictionary, then construct the Message instance in the task.
"""
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html', email=session.get('email', ''))
    email = request.form['email']
    session['email'] = email

    # send the email
    msg = Message('Test Celery with Flask',
                  recipients=[request.form['email']])
    msg.body = 'This is a test email sent from a background Celery task.'

    if request.form['submit'] == 'Send':
        # send right away
        send_async_email.delay(msg)
        flash('Sending email to {0}'.format(email))
    else:
        # send in one minute
        send_async_email.apply_async(args=[msg], countdown=30)
        flash('An email will be sent to {0} in half minute'.format(email))

    return redirect(url_for('index'))


@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    
    # 202 in REST APIs to indicate a request is in progress
    # 'Location' added to response headers
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}


@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)

    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 0),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something wrong happended to the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info)    # when 'FAILURE', task.info will contain exception raised.
        }

    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
