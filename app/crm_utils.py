from dotenv import load_dotenv
from openai import OpenAI
import json, time, re, os
from io import StringIO
from datetime import datetime
import pandas as pd
import logging


#test start to  finish, esp retrieve and fix assistants  version 2

load_dotenv()
client = OpenAI(default_headers={"OpenAI-Beta": "assistants=v2"})

def extract(user_input):

    """
    Extract relevant fields from the provided user input and format the output as a JSON object.

    Parameters:
    - user_input (str): The input text containing information about the new lead

    Returns:
    A dictionary representing the extracted fields
    """
    extraction_prompt = """You are a customer relations manager. 
                        Your job is to track email or text conversations between the user and their clients.
                        The current date and time is: {}
                        The answer should consist of a json object according to the following schema:

                        "contact_name": string // "Extract the client or company name, the user is communicating with, indicated after From: or To:",
                        "message": string // "Extract the main takeaway of the conversation, avoiding excessive information.",
                        "contact_date": string // "Extract the date that the message took place, indicated at the start. If the date is vague, try to match it to a more concrete one. Example: Mid-April -> 15th April. If the date is missing, extract today's date. Desired format: Day-Month-Year, where Year is the current year unless mentioned otherwise.",
                        "medium": string // "Extract the medium of the forthcoming communication. Choose between: in-person meeting, phone call, email, or text. Default to email.",
                        "followup_date": string // Extract which date is the expected followup of the conversation. Desired format: Day-Month-Year, where Year is the current year unless mentioned otherwise"
                        "followup_time": string // Extract the time of the expected followup reminder. If not mentioned, default to '9:00'.,

                        Be consice and consistent. If a field is not mentioned, extract -1.
                        
                        Example of user_input where the extracted fields are in brackets:
                         
                        Date: [contact_date]
                        From: [contact_name] (or To: [contact_name])
                         
                        I have this [message]. Should we get back to it by [medium] around [followup_date]?
                        
                        Kind regards,
                        user


    """

    messages = [{"role":  "system", "content": extraction_prompt.format(datetime.today().strftime("%d-%m-%Y"))},
            {"role": "user", "content": user_input}]

    response = client.chat.completions.create(model="gpt-4o",
                                              messages=messages,
                                              response_format={"type": "json_object" }).choices[0].message.content
    return json.loads(response)

def remind(blob_client, current_date=None):
    """
    Generate notification messages based on specified guidelines.

    Parameters:
        blob_client (azure.storage.blob.BlobClient): BlobClient object where the database file containing lead information.
        current_date (str, optional): The current date and time as a reference. If not provided, the current datetime will be used.

    Returns:
        bool: True if the OpenAI API call was successful, False otherwise.

    """
    prompt = """You manage follow-up reminders for customer leads, notifying the user on scheduled days.
                    Keep a consistent datetime format, specifically Weekday, Date-Month-Year.

                    Do not execute code, only reason to arrive at a solution.
                    The guidelines for sending reminders are as follows:

                    - You must send all reminders that are relevant and/or missed.
                    - Relevant reminders are those whose follow-up date matches today's date, and their reminder_sent flag is set to 'False'.
                    - Missed relevant are those with a followup_date earlier than the current date reference, and their reminder_sent is set to 'False'.
                    - Each reminder should be at most 1-2 lines of text, resembling a mobile app notification.

                    Compose friendly reminder messages including:
                    Contact's name
                    Lead topic
                    Method of Communication
                    Duration since last interaction

                    Example:
                    "Here's a [new/missed] reminder to follow up with [Contact Name] on [Lead Topic]. It's been [X days/weeks] since your last conversation."

                     The answer should EXCLUSIVELY consist of a json object according to the following schema:
                    {
                        "generated_reminders": [string, string...] // A list of the reminders to be sent
                        "reasoning": string, // An elaborate and complete reasoning for the reminders you sent
                        "rows": [int, int, ...]  // List of the rows from the database that correspond to the reminders you sent
                    }
                    """
    # Download the database from blob storage in Azure
    blob = blob_client.download_blob(encoding='utf8').readall()
    blob = pd.read_csv(StringIO(blob))
    
    # Save the database locally as a csv for OpenAI processing
    database_path = os.path.join('data', 'clients.csv')
    blob.to_csv(database_path, index=False, encoding ="utf-8")

    # Upload the file to OpenAI
    db = client.files.create(file=open(database_path, "rb"),
                            purpose='assistants')
    
    # Delete the local file for privacy
    os.remove(database_path)
    
    # Instantiate OpenAI assitant
    assistant = client.beta.assistants.create(
        name="Reminder Assistant",
        instructions=prompt,
        model="gpt-4o",
        tools=[{"type": "code_interpreter"}],
        )
                                               
    # Inform the assistant of current date and time
    todaysdate = f"Access the database for information using this current date and time as reference: {current_date if current_date else datetime.now().strftime('%d-%m-%Y')}"

    thread = client.beta.threads.create()

    message = client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=todaysdate,
        attachments=[{"file_id": db.id, "tools": [{"type": "code_interpreter"}]}]
        )

    # Run the assistant with the thread messages
    run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant.id)
    
    #Wait for the OpenAI assistant to process the prompt & file upload
    while True:
        logging.info('Give the reminder agent 10 more seconds..')
        count = time.sleep(10)  
        
        # Retrieve the run status
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        
        if run_status.status in ['failed', 'expired', 'requires_action']:
            logging.warning(f"Status: {run_status.status}. Error: {run_status.last_error.message}")
        
        # If run is completed, get output messages
        if run_status.status == 'completed':
            messages = client.beta.threads.messages.list(thread_id=thread.id)

            for msg in messages.data[0:1]:  #only get the first message, the rest are usually useless
                try:
                    content = msg.content[0].text.value
                    if msg.role == "assistant":
                        # Extract the json object from the output as openAI assistants do not support structured_responses for now
                        output = json.loads(re.search(r'\{(?:[^{}]|\\{|\\})*\}', content, re.DOTALL).group())
                        try:  
                            for row in output['rows']:
                                blob.at[int(row), 'reminder_sent'] = True #change the database so that the reminder is marked as sent.
                        except:
                            logging.warning("The reminder did not generate a relevant position in the database or something else went wrong.")
                except:
                    # if .text does not exist (assistant did not return messages)
                    logging.warning("No assistant messages could be retrieved.")
            break

def format_text(recipient, text, template_name):
    """
    Formats a message as a whatsapp message according to a template
    These templates were created manually in facebook business templates 

    Args:
        recipient (str): The recipient's WhatsApp ID.
        text (str): The text message to be formatted.
        template_name (str): The name of the template.

    Returns:
        str: JSON-formatted message.
    """
    if template_name == "initiate":
        return json.dumps({
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "template",
            "template": {
                "name": 'add_remind',
                "language": {"code": "en"},
                "components": [{
                    "type": "button",
                     "sub_type": "quick_reply",
                     "index":"0",
                    "parameters": [{
                        "type": "payload",
                        "payload": "Add_button"}
                       ]},
                    {
                    "type": "button",
                    "sub_type": "quick_reply",
                     "index": "1",
                    "parameters": [{
                        "type": "payload",
                        "payload": "Retrieve_button"
                       }]},
                ]
            }
        })
    if template_name == "simple":
        return json.dumps({
            "messaging_product": "whatsapp",    
            "to": recipient,
            "type": "text",
            "text": {
                "body": text
            }
        })
    
    if template_name == "cancel_option":
        return json.dumps({
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {
                    "text": text
                },
                "footer": {
                    "text": "Whatsapp Customer Relations Manager"
                },
                "action": {
                    "buttons": [{
                        "type": "reply",
                        "reply": {
                            "id": "Cancel_button",
                            "title" : "Cancel"
                        }
                    }]
                }
            }
        })


    if template_name == "yes_no":
        return json.dumps({
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {
                    "text": text
                },
                "footer": {
                    "text": "Whatsapp Customer Relations Manager"
                },
                "action": {
                    "buttons": [{
                        "type": "reply",
                        "reply": {
                            "id": "Yes_button",
                            "title" : "Yes"
                        }
                    },
                    {
                        "type": "reply",
                        "reply": {
                            "id": "No_button",
                            "title" : "No"
                        }
                    }
                ]
            }
        }
    })
    if template_name == "info_complete":
        return json.dumps({
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {
                    "text": "Your information is complete. Send another message to edit your information or confirm below:"
                },
                "footer": {
                    "text": "Whatsapp Customer Relations Manager"
                },
                "action": {
                    "buttons": [{
                        "type": "reply",
                        "reply": {
                            "id": "Confirm_button",
                            "title" : "Confirm"
                        }
                    },
                    
                    {
                        "type": "reply",
                        "reply": {
                            "id": "Cancel_button",
                            "title" : "Cancel"
                        }
                    }
                ]
            }
        }
    })

def format_dict(dictionary, keys=["contact_name", "message", "contact_date", "followup_date", "medium"]):
        """
        Format a Python dictionary in a WhatsApp-friendly format.

        Parameters:
        - dictionary (dict): The dictionary to be formatted.

        Returns:
        - formatted_string (str): The formatted string with one key-value pair per line,
        removing underscores from keys and capitalizing the first letter of each key.
        """
        formatted_string = ""
        for key in keys:
            if key in dictionary:
                field = key.replace("_", " ").capitalize()
                formatted_string += f"\n{field}: {dictionary[key]}"
        return formatted_string

def format_reminder(output):
        """
        Format the reminders and the reasoning generated from the reminder model.

        Parameters:
        - output (dict): A dictionary containing the generated reminders, reasoning, and row number.

        Returns:
        - formatted_reminder (str): The formatted reminders, including the reasoning from the model for choosing them
        - formatted_reasoning (str): The formatted reasoning.
        - row_number (int): The row number that the generated reminder corresponds to in the database.
        """
        try:
            if output is not None and output['generated_reminders']:
                formatted_reminder = "\nGenerated Reminders:\n"
                formatted_reminder += "".join(f"- {reminder}" for reminder in output['generated_reminders']) + "\n"
                formatted_reasoning = "\nReasoning:\n" + str(output['reasoning'])
            else:
                formatted_reminder = "No reminders for now.\n"
                formatted_reasoning = ""

            return formatted_reminder + formatted_reasoning
        
        except KeyError:
            return "Could not retrieve reminders.\n"