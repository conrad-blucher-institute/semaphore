from datetime import datetime


class DataPoint():
    def __init__(self, value: str, unit: str, dateTime: datetime) -> None:
        self.value = value
        self.unit = unit
        self.dateTime = dateTime

    def __str__(self) -> str:
        return f'[DataPoint] -> value: {self.value}, unit: {self.unit}, dataTime: {self.dateTime}'


class Prediction():
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime, successValue: str = None) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
        self.successValue = successValue
    
    def __str__(self) -> str:
        return f'[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}'


class Request():
    def __init__(self, source: str, series: str, location: str, unit: str, isPrediction, fromDateTime: datetime, toDateTime: datetime, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.location = location
        self.unit = unit
        self.datum = datum
        self.isPrediction = isPrediction
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime

    def __str__(self) -> str:
        return f'[Request] -> source: {self.source}, series: {self.series}, location: {self.location}, unit: {self.unit}, datum {self.datum}, isPrediction {self.isPrediction}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'


class Response():
    def __init__(self, source: str, series: str, location: str, unit: str, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.location = location
        self.unit = unit
        self.datum = datum
        self.data = []

    def __str__(self) -> str:
        return f'[Response] -> source: {self.source}, series: {self.series}, location: {self.location}, unit: {self.unit}, datum {self.datum}, data {self.data}'
