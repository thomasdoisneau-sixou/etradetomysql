# want: collect all the functions in one place
# automate Etrade authentication (by generating access tokens) and ETL process from a Flask server

import os 
from signal import SIGTERM, SIGKILL 
from flask import Flask, request 
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client 
from pyngrok import ngrok 

import undetected_chromedriver as uc
import pyetrade 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from params import account_sid, auth_token, twilio_number, to_number, consumer, username, cst_password, sat2
from ppc_functions import sunday_pg

def try_code():
    '''Asks for the code, then tries to use it to get the tokens and run the program. If this works, the user is notified and the process is killed; if this doesn't work, the user is asked to try again. The process can be killed at any time by sending 'Shutdown'.
    Sends a message asking for the code. Then, starts a Flask server to listen for incoming messages. Incoming messages are parsed to extract their body, which is used to determine the next step. If the body is 'Shutdown', the process is killed. If not, the incoming message's body is used to try to get tokens and run the program. If this is successful, a response informing that the progress was run successfully is sent back, and the process is killed. If not, the exception is caught and a response asking for a new code is sent back.'''
    app = Flask(__name__)
    
    route = '/'
    client = Client(account_sid, auth_token)
    
    @app.route(route, methods = ['POST'])
    def try_code():
        code = request.values.get('Body', None)
        print(code)
        
        resp = MessagingResponse()
        
        if code.lower() == 'shutdown':
            send_msg(client, 'Shutting down...')
            print('Shutting down...')
            kill_process()
                
        try:
            tokens = get_tokens(code)
            print(tokens)
            sunday_pg(tokens, sat2)
            
            send_msg(client, 'Success: the program was run.\nShutting down...')
            print('Shutting down...')
            kill_process()
        
        except TimeoutException:
            print('TimeoutException raised.')
            send_msg(client, 'A TimeoutException was raised. Did you enter the correct code?')
            
        except Exception as e:
            print('exception')
            send_msg(client, f'An exception was raised: {e}.\nPlease try again.')
        
        return str(resp) # try: Null. Doesn't work. try: str(MessagingResponse())
    
    if __name__ == '__main__':
        if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
            start_ngrok(client, route)
        send_msg(client, 'Please enter the code.')
        app.run(debug = True) # change this for prod
        
        
def get_tokens(code):
    '''Input: six-digit variable code -> output: Etrade access tokens.
    Takes the variable code (required by Etrade MFA). Then, generates the url that is used to get the Etrade verification code. Makes an undetected chromedriver object (a Selenium ChromeDriver modified to fool bot-detection software) and uses it to log in to Etrade, click on 'Accept' (waiting a given number of seconds to do so, if the element is not immediately available) and scrape the verification code. Finally, uses the verification code to generate Etrade access tokens (valid for a day) and returns these.
    '''
    oauth = pyetrade.ETradeOAuth(consumer['key'], consumer['secret'])
    url = oauth.get_request_token()
    
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    driver = uc.Chrome(options = options, use_subprocess = True) # latter arg addresses RuntimeError

    driver.get(url)
    username_input = driver.find_element('name', 'USER')
    password_input = driver.find_element('name', 'PASSWORD')
    username_input.send_keys(username)
    password = cst_password + str(code) # construct password
    password_input.send_keys(password)
    login = driver.find_element('id', 'logon_button')
    login.click()
    print('Clicked log on button.')

    xpath = "/html/body/div[2]/div/div[2]/form/input[3]"
    seconds_wait = 5
    wait = WebDriverWait(driver, seconds_wait)
    wait.until(EC.element_to_be_clickable(('xpath', xpath)))
    accept = driver.find_element('xpath', xpath)
    accept.click()
    print('Got past Accept terms page.')
    verification_code = driver.find_element('tag name', 'input').get_attribute('value')
    print(verification_code)
    tokens = oauth.get_access_token(verification_code)
    
    return tokens

        
def start_ngrok(client, route):
    '''Opens ngrok tunnel programmatically and updates Twilio SMS webhook using specified Twilio client and Flask route (to map ngrok tunnel URL to the Flask function).'''
    url = ngrok.connect(5000).public_url
    print(' * Tunnel URL:', url)
    client.incoming_phone_numbers.list(
        phone_number = twilio_number 
    )[0].update(sms_url = url + route)
    
    
def send_msg(client, message):
    '''Sends specified SMS message to phone number using specified Twilio client. 
    Makes client object (using account details imported from a parameters file) and creates message object.'''
    client.messages.create(body = message,
                           from_ = twilio_number,
                           to = to_number
                           )

                           
def kill_process():
    '''Identifies current process by its process identifier (PID) and kills it.
    Attempts graceful termination; if unsuccessful, executes brute-force termination.'''
    pid = os.getpid()
    try:
        os.kill(pid, SIGTERM)
    except Exception:
        os.kill(pid, SIGKILL)
        
        

   
       
try_code()