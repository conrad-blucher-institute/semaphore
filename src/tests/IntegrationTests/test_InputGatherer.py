
    
def test_generateInputSpecifications():
    """This function tests whether __generate_inputSpecifications
    generates the correct input specifications from the specified
    DSPEC file
    """
    currentDate = datetime.now()

    dspecFileName = 'test_dspec.json'

    inputGatherer = InputGatherer(dspecFileName)
    
    inputGatherer._InputGatherer__generate_inputSpecifications(currentDate)

    inputSpecifications = inputGatherer.get_input_specifications()

    specification = inputSpecifications[0]
    seriesDescription = specification[0]
    timeDescription = specification[1]
    dataType = specification[2]

    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName
    if not path.exists(dspecFilePath):
        log(f'{dspecFilePath} not found!')
        raise FileNotFoundError
    
    with open(dspecFilePath) as dspecFile:
        json = load(dspecFile)

        inputsJson = json["inputs"]

        assert seriesDescription.dataSource == inputsJson[0]["source"]
        assert seriesDescription.dataSeries == inputsJson[0]["series"]
        assert seriesDescription.dataLocation == inputsJson[0]["location"]
        assert seriesDescription.dataDatum == inputsJson[0].get("datum")
        assert timeDescription.interval == timedelta(seconds=inputsJson[0]["interval"])
        assert dataType == inputsJson[0]["type"]