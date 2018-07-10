import smtplib
from email.mime.text import MIMEText

from fetchWeather import fetchWeather

# basic set up
fromAdd = "clr_paradise@163.com" # from which email address
toAdd = "echo333cheng@gmail.com" # to which email address
host = 'smtp.163.com' # email host, need to replace
username = fromAdd # from which email address
password = input('Enter password:') # type in password to log in the account
postfix = '163.com' # need to replace

# a function to send emails
def sendMail(toAdd, sub, content):
    
    me = 'hello' + '<' + username + '@' + postfix + '>'
    msg = MIMEText(content, _subtype = 'plain')
    msg['Subject'] = sub
    msg['From'] = fromAdd
    msg['To'] = toAdd

    try:
        server = smtplib.SMTP()
        server.connect(host)
        server.login(username, password)
        server.sendmail(me, toAdd, msg.as_string())
        server.close()
        return True
    except:
        return False

# send email and get sending result
for i in range(1):
    sub = 'Weather Forecast'
    content = fetchWeather()
    if sendMail(toAdd, sub, content):
        print('Succeeded')
    else:
        print('Failed')
