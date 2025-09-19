import os
import asyncio
import aiohttp
import logging
import uvicorn
import requests
import dateparser
import editdistance
import pandas as pd
from io import StringIO
from datetime import date
from dotenv import load_dotenv
from dataclasses import dataclass
from fastapi import FastAPI, Request
from azure.storage.blob import BlobServiceClient, BlobClient
from crm_utils import extract, remind, format_text, format_dict, format_reminder

app = FastAPI()
messages = []

#Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(filename)s - %(message)s \n')
logging.getLogger("crm-app").setLevel(logging.INFO)

# Load environmental variables
load_dotenv()

variables = [
    "OPENAI_API_KEY",
    "APP_ID",
    "APP_SECRET",
    "RECIPIENT_WAID",
    "VERSION",
    "PHONE_NUMBER_ID",
    "ACCESS_TOKEN",
    "WEBHOOK_VERIFY_TOKEN",
    "CONNECTION_STRING"
]
config = {var: os.getenv(var) for var in variables}

@app.get("/") 
async def root():
    return {"Nothing to see here. Make a request to the /crm endpoint to run the CRM app."}

@app.get("/webhook") 
async def verify_webhook(request: Request): 
    # Accessing query parameters from the request object
    hub_mode = request.query_params.get("hub.mode")
    hub_verify_token = request.query_params.get("hub.verify_token")
    hub_challenge = int(request.query_params.get("hub.challenge"))

    # check the mode and token sent are correct
    if hub_mode == "subscribe" and hub_verify_token == config["WEBHOOK_VERIFY_TOKEN"]:
        logging.info("Token verified.")
        # respond with 200 OK and challenge token from the request
        return hub_challenge
    else:
        # respond with '403 Forbidden' if verify tokens do not match
        logging.info(f"hub_mode: {hub_mode}, hub_verify_token: {hub_verify_token}, hub_challenge: {hub_challenge}")
        logging.info(f"Expected token: {config['WEBHOOK_VERIFY_TOKEN']}")
        return {"error": "Invalid token or mode"}, 403

@app.post("/webhook")
async def webhook(request: Request):
    req = await request.json()
    logging.info(f"Incoming webhook message: {req}")
    message = req.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("messages", [{}])[0]

    if message.get("type", 0) in ["text", "interactive", "button"]:
        messages.append(message)

        # Mark incoming message as read
        response = requests.post(
            f"https://graph.facebook.com/{config['VERSION']}/{config['PHONE_NUMBER_ID']}/messages",
            headers={"Authorization": f"Bearer {config['ACCESS_TOKEN']}"},
            json={"messaging_product": "whatsapp", "status": "read", "message_id": message["id"]},
        )
        response.raise_for_status()
    return {"status": "ok"}

@app.get("/messages") 
async def get_messages():
    global messages
    stored_messages = messages
    messages = []
    return stored_messages

async def send(session, message, template_name="simple"):
    """
    Sends a message using the Facebook Graph API.

    Args:
        session: aiohttp.ClientSession object.
        message (str): The message to be sent.
        template_name (str, optional): The name of the template to use. Defaults to "simple".
    """
    logging.info(f"Output to the user [template: {template_name}]: {message}")

    data = format_text(config['RECIPIENT_WAID'], message, template_name)
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {config['ACCESS_TOKEN']}",
    }

    url = f"https://graph.facebook.com/{config['VERSION']}/{config['PHONE_NUMBER_ID']}/messages"
    try:
        async with session.post(url, data=data, headers=headers) as response:
            if response.status == 200:
                html = await response.text()
            else:
                logging.warning(f"Something went wrong: {response.status}")
            return response.status
    except aiohttp.ClientConnectorError as e:
        logging.warning(f"Connection Error {str(e)}")

async def receive(session):
    """
    Receives messages from the webhook server.

    Args:
        session: aiohttp.ClientSession object.
        url (str, optional): The endpoint to fetch messages from.".

    Returns:
        str: Concatenated messages received.
    """
    message_list = None

    while True:
        async with session.get("https://app-crmagent-dev-001.azurewebsites.net/messages") as response:
            if response.status == 200:

                messages = await response.json()
                message_list =  [msg['text']['body'] if msg['type'] == 'text' else msg['interactive']['button_reply']['id'] if msg['type'] == 'interactive' else msg['button']['payload'] if msg['type'] == 'button' else 'unrecognized message' for msg in messages]
                logging.info(f"Successfully fetched: {message_list}")
                
                if message_list:
                    return " ".join(message_list)
                await asyncio.sleep(10)
            else:
                logging.warning(f"Failed to retrieve messages: {response.status, response}")

@app.get("/crm") 
async def main():

    #Store the information of a single lead
    @dataclass
    class Lead:
        contact_name: str = None
        message: str = None
        contact_date: date = None
        medium: str = None
        followup_date: date = None
        followup_time: str = None
        reminder_sent: bool = False

        p_success: float = 1.0
        payoff: float = 0.0
        
        async def update(self, data):

            """
            Update lead information based on the given data.
            It skips uninformative updates such as None and '-1' values
            It can parse variable strings as datemine objects
            Note: The last 3 attributes are not updated for now.

            Args:
                data (dict): Dictionary containing lead information.

            Returns:
                None
            """
            # Model's output parser must return a json
            if not isinstance(data, dict):
                return
            for key, value in data.items():
                if value is not None and value != '-1' and value != -1:
                    try:
                        if key == 'contact_date' or key == 'followup_date':
                            date=dateparser.parse(value, settings={'DATE_ORDER': 'DMY'}).strftime('%d-%m-%Y')
                            setattr(self, key, date)

                        elif key == 'followup_time':
                            time=dateparser.parse(value, settings={'DATE_ORDER': 'DMY'}).strftime("%H:%M")
                            setattr(self, key, time)
                        else:
                            setattr(self, key, value)
                    
                    except Exception as e:
                        await send(session, f"Could not set {key} as {value}. Try again.")

    # Create lead class
    curr_lead = Lead()


    async def match_contact(session, existing_contacts, new_contact):

        """
        Match a contact reference to existing contacts.
        Exact matches are prioritized, then with edit distance < 3
        If no close matches are found, permission is requested to create new contact.
        Note: this function does not handle user_input that it not 'y' or 'n' well.
        
        Args:
            existing_contacts (List[str]): List of existing contacts.
            new_contact (str): New contact to match.

        Returns:
            str: Matched or new contact name.

        """
        for contact in existing_contacts:
            if contact is not None:
                dist = editdistance.eval(contact, new_contact)
                # Exact matches
                if dist == 0:
                    await send(session, f"Found {contact} as an existing contact.")
                    return contact

                # edit distance < 3
                elif dist < 3:
                    await send(session, f"Did you mean '{contact}' ?", template_name="yes_no")
                    user_input = await receive(session)
                    if user_input == 'Yes_button':
                        return contact
                    elif user_input == 'No_button':
                        continue

        # Confirmation permission for new contact
        await send(session, f"Do you want to create new contact '{new_contact}'?", template_name="yes_no")
        user_input = await receive(session)

        if user_input == 'Yes_button':
            # Create new contact
            await send(session, f"New contact {new_contact} has been created")
            return new_contact
        
        elif user_input == 'No_button':
            # Permission denied, display all past contacts
            await send(session, f"This is a list of your existing contacts: {set(existing_contacts)} \n Which one did you mean?")
            user_input = await receive(session)
            await match_contact(session, existing_contacts, user_input)
        else:
            return None

    async def write_to_db(blob_client, lead):
        """
        Add a new row to the database given a lead object. If the db does not exist, it is created
        This function calls match_contact to compare the contact_name value to previously logged names.

        Args:
            session: aiohttp.ClientSession object for sending status messages to the server.
            blob_client (azure.storage.blob.BlobClient): BlobClient object where the user csv is stored
            lead (dict): Dictionary containing the attributes of the Lead dataclass
        
        Returns:
            None
        """


        # Define headers
        headers = ['contact_name', 'message', 'contact_date', 'followup_date', 'followup_time', 'reminder_sent', 'medium', 'p_success', 'payoff', 'weighted_payoff']
        # Download existing database
        try:
            blob = blob_client.download_blob(encoding='utf8').readall()
            db = pd.read_csv(StringIO(blob))
            logging.info("Retrieved database.")
            # Check if the cotnact already exists
            existing_contacts = list(set(db['contact_name']))
            #contact = await match_contact(session, existing_contacts, lead['contact_name']
            logging.info(f"Found contacts: {existing_contacts}")
            contact = lead['contact_name'] #edit: call match_contact instead 

        # In case the databse does not exist yet
        except:
            db = pd.DataFrame(columns = headers)
            logging.info("Creating new database.")
            contact = lead['contact_name']

        # Extract data from Lead object
        data = {'contact_name': contact,
            'message': lead['message'],
            'contact_date': lead['contact_date'],
            'followup_date': lead['followup_date'],
            'followup_time': lead['followup_time'],
            'reminder_sent': lead['reminder_sent'],
            'medium': lead['medium'],
            'p_success': lead['p_success'],
            'payoff': lead['payoff'],
            'weighted_payoff': lead['payoff'] * lead['p_success']
        }
        
        # Append data to the databse
        db.loc[len(db)] = data
        logging.info(db)
        # Convert to csv file
        blob = db.to_csv(index=False, encoding = "utf-8")
        # Upload to the blob storage
        blob_client.upload_blob(blob, blob_type="BlockBlob", overwrite=True)
        logging.info("Upload successful.")

    async def crm(session, blob_client):
        "Adding new information in the database"
        while True:
            # Inform the user of the tracked informations
            await send(session, f"Logged information: {format_dict(vars(curr_lead))}")

            # Check if all the necessary info is logged
            complete = all(value != -1 and value is not None for value in vars(curr_lead).values())

            if complete:
                await send(session, "", template_name="info_complete")
                user_info = await receive(session)
                
            else:
                await send(session, "Any news?", template_name="cancel_option")
                user_info = await receive(session)
            
            # If the user has confirmed the lead, schedule a reminder
            if user_info == 'Confirm_button':
                logging.info("Confirmed")
                await send(session, "Your database has been updated. Have a good day!", template_name="simple")
                await write_to_db(blob_client, vars(curr_lead))

                break

            elif user_info == 'Cancel_button':
                logging.info("Cancelled")
                break

            #if the user keeps editing, update the data
            else:
                output = extract(user_info)
                await curr_lead.update(output)

# Main Conversation Loop
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200)) as session:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(config['CONNECTION_STRING'])
            container_client = blob_service_client.get_container_client("clients")
            blob_client = container_client.get_blob_client(f"{config['RECIPIENT_WAID']}.csv")
        except:
            raise Exception("Could not establish connection with BlobServiceClient")

        await send(session, "", template_name="initiate") 
        
        user = await receive(session)

        if user == 'Add_button':
             await crm(session, blob_client)

        #function = "Retrieve reminders"
        elif user == 'Retrieve_button':
            output = remind(blob_client) 
            reminder = format_reminder(output)
            logging.info(reminder)
            await send(session, reminder)
        else:
            await send(session, "Invalid request. Start a new session.")

uvicorn.run(app, host="0.0.0.0", port=8000)

asyncio.run(main())

