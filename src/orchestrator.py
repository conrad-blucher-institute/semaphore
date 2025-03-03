# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Matthew Kastl
# version 1.0
#----------------------------------
""" 
This file handles the following:
    Orchestrating semaphore components to run correctly.
    Provide a safe environment to catch any exceptions and ensure notifications and logging occurs properly.
    Safe results or lack there of to the database.
 """ 
#----------------------------------
# 
#
#Imports
from os import path, getenv
from datetime import datetime, timedelta
from exceptions import Semaphore_Exception, Semaphore_Data_Exception, Semaphore_Ingestion_Exception
from discord import Discord_Notify
from DataClasses import Series, SemaphoreSeriesDescription, Output
from utility import log, LogLocationDirector

from SeriesStorage.ISeriesStorage import series_storage_factory
from ModelExecution.dataGatherer import DataGatherer
from ModelExecution.InputVectorBuilder import InputVectorBuilder
from ModelExecution.modelWrapper import ModelWrapper
from ModelExecution.dspecParser import DSPEC_Parser, Dspec


class Orchestrator:

    def __init__(self) -> None:
        self.DSPEC_parser = DSPEC_Parser()
        self.dataGatherer = DataGatherer()
        self.inputVectorBuilder = InputVectorBuilder()
        self.modelWrapper = ModelWrapper()


    def run_semaphore(self, dspecPaths: list[str], executionTime: datetime = None, toss: bool = False):
        """Runs one dspec through semaphore protected to catch any possible error and handle results accordingly
            - Parses the DSPEC file(s)
            - Calculates the reference time
            - For each DSPEC 
                - Gathers the data
                - Sets the log target
                - Gathers data
                - Builds the input vectors
                - Makes the prediction
                - Handles the successful prediction
        NOTE: If a prediction fails, it will be handled by the __handle_failed_prediction method. This method will ensure that 
            a notification is sent and that the result is logged in the database.

        :param dspecPaths: list[str] - A list of paths to the DSPEC files that will be run through semaphore.
        :param executionTime: datetime - The time Semaphore should build reference time off of. If not provided, 
            the current time will be used.
        :param toss: bool - A flag that will prevent the computed prediction from actually being saved in the database.
        """
        model_name = None
        DSPEC = None

        # By default the DSPECs will be ordered left -> right from the command
        checked_dspecs = [self.__clean_and_check_dspec(dspec) for dspec in dspecPaths]
        if executionTime is None: 
                    executionTime = datetime.now()
        reference_time = self.__calculate_referenceTime(executionTime, DSPEC)

        try:
            try:
                for dspecPath in checked_dspecs:
                
                    DSPEC = self.DSPEC_parser.parse_dspec(dspecPath)
                    model_name = DSPEC.modelName

                    LogLocationDirector().set_log_target_path(getenv('LOG_BASE_PATH'), model_name)
                    log(f'----Running {dspecPaths} for {executionTime}! Toss: {toss}----')

                    data_repository = self.dataGatherer.get_data_repository(DSPEC, reference_time)
                    input_vectors = self.inputVectorBuilder.build_batch(DSPEC, data_repository)
                    result = self.modelWrapper.make_prediction(DSPEC, input_vectors, executionTime)

                    self.__handle_successful_prediction(model_name, reference_time, result, toss)
                
            except Exception:
                    raise Semaphore_Exception('Error:: An unknown error ocurred!')
            
        except Semaphore_Exception as se:
            log(f'Error:: Prediction failed due to Semaphore Exception\n{se}')
            self.__handle_failed_prediction(se, reference_time, model_name, DSPEC, toss)
        except Semaphore_Data_Exception as sde:
            log(f'Warning:: Prediction failed due to lack of data.')
            self.__handle_failed_prediction(sde, reference_time, model_name, DSPEC, toss)
        except Semaphore_Ingestion_Exception as sie:
            log(f'Error:: Prediction failed due to Semaphore Ingestion Exception\n{sie}')
            self.__handle_failed_prediction(sie, reference_time, model_name, DSPEC, toss)

    
    def __clean_and_check_dspec(self, dspec_path: str) -> str:
        """Checks that a depsc path ends with the .json extension and that the file actually exists.
        
        :param dspec_path: str - The path to the dspec file.
        :returns: str - The path to the dspec file.
        :raises FileNotFoundError: If the file does not exist.
        """
        #checking that dspec file passed exists in the dspec folder
        if not dspec_path.endswith('.json'): 
            dspec_path = dspec_path + '.json'

        if not path.exists(dspec_path):
            raise FileNotFoundError
        return dspec_path
    

    def __calculate_referenceTime(self, execution_time: datetime, dspec: Dspec) -> datetime:
        ''' This function clamps execution time to the nearest interval of the specified in the dspec.
        NOTE:: Calculation execution_time - mod(execution_time, interval)
        
        :param execution: datetime -the execution time.
        :param dspec: Dspec - the dspec object.
        :returns: datetime - the reference time.
        '''
        return datetime.utcfromtimestamp(execution_time.timestamp() - (execution_time.timestamp() % dspec.timingInfo.interval))


    def __handle_successful_prediction(self, model_name: str, execution_time: datetime, result_series: Series, toss: bool):
        """Handels a successful run of semaphore, sending a notification and placing the result in the database.
                - Safely sends a discord notification about the successful prediction.
                - If the toss flag is not set, it will attempt to store the prediction in the database.

        :param model_name: str - The name of the model that was run.
        :param execution_time: datetime - The time the model was run.
        :param result_series: Series - The series of results that were predicted.
        :param toss: bool - A flag that will prevent the computed prediction from actually being saved in the database.
        """
    
        self.__safe_discord_notification(model_name, execution_time, 0, '')

        try:
            if not toss:
                series_storage = series_storage_factory()
                inserted_results, _ = series_storage.insert_output_and_model_run(result_series, execution_time, 0)

                log(inserted_results)
                log(inserted_results.data if inserted_results is not None else '')
        except:
            log(Semaphore_Exception('ERROR:: An error occurred while trying to interact with series storage from semaphoreRunner'))    


    def __handle_failed_prediction(
                                self,
                                exception: Semaphore_Exception | Semaphore_Data_Exception | Semaphore_Ingestion_Exception, 
                                execution_time: datetime,
                                model_name: str | None, 
                                dspec: Dspec | None, 
                                toss: bool
                                ):
        """Handles a failed predictions. This function handles semaphore in an unstable error state. It emits a notification and 
        ensures something is logged in the database.
            - Safely sends a discord notification about the failed prediction.
            - If the toss flag is not set, it will attempt to store a Null prediction in the database.

        :param exception: Semaphore_Exception | Semaphore_Data_Exception | Semaphore_Ingestion_Exception - The exception that occurred.
        :param execution_time: datetime - The time the model was run.
        :param model_name: str | None - The name of the model that was run or None if it was not discovered.
        :param dspec: Dspec | None - The dspec object that was run or None if it was not parsed yet
        :param toss: bool - A flag that will prevent the computed prediction from actually being saved in the database.
        """

        if execution_time is None: execution_time = datetime.min # Execution time could not be instantiated yet
        if model_name is None: model_name = 'Model Name Not Discovered Yet' # Model name might not be discovered if error was in dspec

        self.__safe_discord_notification(model_name, execution_time, exception.error_code, exception.message)

        try:
            if not toss and dspec is not None:
            
                # Reconstruct the expected predictions information with a nulled result
                predictionDesc = SemaphoreSeriesDescription(dspec.modelName, dspec.modelVersion, dspec.outputInfo.series, dspec.outputInfo.location, dspec.outputInfo.datum)
                result_series = Series(predictionDesc, True)
                result_series.data = [Output(None, dspec.outputInfo.unit, self.__calculate_referenceTime(execution_time), timedelta(seconds=dspec.outputInfo.leadTime))]

                # Attempt to store both that in the outputs table and information about the model_run
                series_storage = series_storage_factory()
                inserted_results, _ = series_storage.insert_output_and_model_run(result_series, execution_time, exception.error_code)

                log(inserted_results)
                log(inserted_results.data if inserted_results is not None else '')
        except:
            log(Semaphore_Exception('ERROR:: An error occurred while trying to interact with series storage from semaphoreRunner'))
    

    def __safe_discord_notification(model_name: str, execution_time: datetime, error_code: int, message: str):
        """Safely sends a discord notification if the user has enabled it in the environment variables. Ensures 
        that if an error occurs while sending the notification, it is logged and not thrown and this method returns.

        :param model_name: str - The name of the model that was run.
        :param execution_time: datetime - The time the model was run.
        :param error_code: int - The error code of the error that occurred.
        :param message: str - The message of the error that occurred.
        :logs Semaphore_Exception: If an error occurs while trying to send a discord notification.
        """
        try: 
            if int(getenv('DISCORD_NOTIFY')) == 1: # Discord notification enabled in env
                if int(getenv('IS_DEV')) == 1: # is dev server or prod server
                    webhook = getenv('DISCORD_WEBHOOK_DEV')
                else:
                    webhook = getenv('DISCORD_WEBHOOK_PROD')

                # send notification
                discord_notify = Discord_Notify(webhook)
                discord_notify.send_notification(model_name, execution_time, error_code, message)
        except Exception as e:
            log(Semaphore_Exception('ERROR:: An error occurred while trying to send a discord notification'))
