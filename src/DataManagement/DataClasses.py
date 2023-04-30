from datetime import datetime
class DataPoint():
    def __init__(self, value: str, unit: str, dateTime: datetime) -> None:
        self.value = value
        self.unit = unit
        self.dateTime = dateTime


class Prediction():
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime, successValue: str = None) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
        self.successValue = successValue

class Request():
    def __init__(self, location: str, source: str, series: str, unit: str, isPrediction, fromDateTime: datetime, toDateTime: datetime, datum: str = None) -> None:
        self.location = location
        self.source = source
        self.series = series
        self.unit = unit
        self.datum = datum
        self.isPrediction = isPrediction
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime

class Response():
    def __init__(self, location: str, source: str, series: str, unit: str, datum: str = None) -> None:
        self.location = location
        self.source = source
        self.series = series
        self.unit = unit
        self.datum = datum
        self.data = []
