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
from os import getenv

class Discord_Notify:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

        # Ids are in the order 
        DEV_THREAD_IDS = {"DI_failure": 1289283580823863437, "sem_failure": 1289283697325113444, "success": 1289283843840540809}
        PROD_THREAD_IDS = {"DI_failure": 1289283334035214367, "sem_failure": 1289283470438436985, "success": 1289282912105136151}

        if int(getenv('IS_DEV')) == 1:
            utilized_thread_IDS = DEV_THREAD_IDS
        else:
            utilized_thread_IDS = PROD_THREAD_IDS


        self.webhook_parameters = {
            -1 : {
                'thread_id': utilized_thread_IDS['DI_failure'],
                'color': 'fcba03',
                'base_desc': 'Model failed to run due to lacking data. Extra info provided: '
            },
            0 : {
                'thread_id': utilized_thread_IDS['success'],
                'color': '03fc2c',
                'base_desc': 'Model ran successfully.'
            },
            1 : {
                'thread_id': utilized_thread_IDS['sem_failure'],
                'color': 'fc0303',
                'base_desc': 'Model failed to run due to ingestion error. Extra info provided: '
            },
            2 : {
                'thread_id': utilized_thread_IDS['sem_failure'],
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

