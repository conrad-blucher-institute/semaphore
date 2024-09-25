# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 09/25/2024
# version 1.0
#----------------------------------
""" A file to send notification to a discord webhook
    the main method uses an error code to decide what 
    thread and message style to use.
 """ 
#----------------------------------
# 
#
#Imports
from discord_webhook import DiscordWebhook, DiscordEmbed
from datetime import datetime
from utility import log

class Discord_Notify:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.webhook_parameters = {
            -1 : {
                'thread_id': 1288529369886883840,
                'color': 'fcba03',
                'base_desc': 'Model failed to run due to lacking data. Extra info provided: '
            },
            0 : {
                'thread_id': 1288525052802764933,
                'color': '03fc2c',
                'base_desc': 'Model ran successfully. Extra info provided: '
            },
            1 : {
                'thread_id': 1288529471665602671,
                'color': 'fc0303',
                'base_desc': 'Model failed to run due to ingestion error. Extra info provided: '
            },
            2 : {
                'thread_id': 1288529471665602671,
                'color': 'fc0303',
                'base_desc': 'Model failed to run due to semaphore error. Extra info provided: '
            },
        }

    def send_notification(self, model_name: str, reference_time: datetime, error_code: int, description_info: str):


        webhook = DiscordWebhook(self.webhook_url, rate_limit_retry=True, thread_id= self.webhook_parameters[error_code]['thread_id'])

        # create embed object for webhook
        # you can set the color as a decimal (color=242424) or hex (color="03b2f8") number
        embed = DiscordEmbed(title=f'{model_name} - {datetime.strftime(reference_time, "%b %d %Y %I:%M%p")}', description= self.webhook_parameters[error_code]['base_desc'] + description_info, color=self.webhook_parameters[error_code]['color'])

        # add embed object to webhook
        webhook.add_embed(embed)

        response = webhook.execute()
        if response.status_code != 200:
            log(f'Warning:: 200 was not reached for discord notification. {response}')

