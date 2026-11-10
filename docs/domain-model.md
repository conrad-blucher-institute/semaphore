This page documents the domain model for Semaphore. The Semaphore domain model had evolved a lot since we first designed the platform. The current domain model is based on our experience operationalizing a variety of predictive models having a variety of requirements ranging from input creation (including data ingestion), "model" structure (i.e.,  member count, Input count, output count), to output requirements. The current domain model is not entirely reflected in the current implementation but is at least conceptually representative of the current implementation. The purpose of creating such a domain model is to help in determining areas of Semaphore that would benefit from some refactoring to simplify it and make it easier to monitor and maintain as well as to make it easier to operationalize future models.

It makes sense to partition the domain model into 2 sub-domains: model execution and input vector creation.

# Model Execution
This sub-domain is all about executing models and recording their output.
 

| Name | Definition | Notes |
| ----| ---- | ---- | 
| **Verified Time** | For predictions, quite literally, the time at which we will be able to verify the prediction.  Practically this means the time for which we are making the prediction. | For measurements, the verified time is the time at which the measurement was taken. |
| **Reference Time/Generated Time** | The time at which a prediction is made. | For measurements this is always the same as the verified time. |
| **Lead Time** | The delta between the reference time and the verified time. Basically, for predictions, how long in advance was the prediction made. | Lead time is always 0 or None for measurements |
| **Model** | A neural net model that generates predictions for a particular lead time, location and predicted variable, a particular set of inputs and is represented by a given set of weights (typically one or more tensorflow h5 or keras files) | In the context of Semaphore, 2 predictive models that are for the same lead time and location and predicted variable, and that use the same input vectors but are represented by distinct weights/tensorflow files are considered 2 distinct models.  A model may be be an Ensemble Model or a Simple Model. |
| **Ensemble Model** | A specific type of model that contains multiple members. | The execution of this type of model consists in running each member in the ensemble against the ensemble input vector and gathering all resulting values into a single output.|
| **Ensemble Member** (or member for short) | A single member of an ensemble model | Each member in a ensemble model has an associated set of weights (h5 or keras file) |
| **Simple Model** | A specific type of model that contains a single members. | Basically a special case of an ensemble model where there are only one ensemble member.|
| **Model Dimensions** | A 3-tuple specifying the 3 key aspects of a predictive model: the number of ensemble members (if not an ensemble model, this number will be 1), the number of input vectors to feed the model (this determines the number of times each ensemble member will be run, once per input vector) and the number of outputs generate by each member run. | For example, a model dimensions of (1,1,1) represents a model that has a single member, where the single member is executed against a single input vector, and generates a single output per member run. A model dimensions of (10, 10, 100) represents a model that has 10 members, where each member is executed against 10 different input vectors, and where each member run generates 100 outputs. |
| **Ensemble Input Vector** | The set of input vectors that is used to execute a model. Models that use more than one input vector do so typically to predict a range of values based on a range of possible input vectors (e.g., a range of air temperature predictions) | A model is executed by feeding each of its ensemble member each of the input vector in the ensemble input vector and recording all of the outputs. Some models only use a single input vector per ensemble member run. In this case the set has only one input vector member|
| **Input Vector Member** | A single member of an Ensemble Input Vector, represented by a single vector (1 dimensional array) in the application | The ordering of the data in an input vector member is crucial. See the `Input Vector Creation` section for more information. |
| **Model Run**| The act of executing a model to generate an output. Consists in executing each ensemble member against an ensemble input vector. Key properties of a model run include:  the model that was run, when the model was run, the reference time for the run, what outputs the model produced if any, whether the run was successful and, if not, why it failed | Although, at first glance, it seems that the primary concept in our domain model is the concept of a predictive model, in reality, Semaphore's primary concern is executing predictive models to generate outputs. Hence, the concept of a model run is key in the Semaphore implementation.|
|**Member Run**| The act of executing a single member of a model against an ensemble input vector. | This concept is important when discussing model run logic and implementation. However, it is not explicitly represented nor persisted in Semaphore as it is always part of a model run and cannot exist outside of a model run. It is implicitly represented in the shape of the output produced by a model run.|
| **Member Run Output**| Each member run results in the generation of an output. This output can be a single value or a set of values. For example, a member run might only generate one value representing the prediction of an environment variable (e.g., water level) or could generate a set of predictions. Regardless of whether it is a single value or a tuple, the output generated by the member is called a member output. | Semaphore does not restrict the type or number of values in a member run output. |
| **Model Run Output** | The set of member run outputs generated by a model run. | In the case of ensemble models, this can be a lot of values. For example, for a model of dimensions (10,10,100) a single model run output contains 10,000 values. For a model with dimensions (10, 100,100) a model run output contains 100,000 values. |
| **Successful Model Run** | A model run is considered successful if it produces an output | Useful for reporting current status of a model. Note that success, in this context, has nothing to do with the accuracy of the predictions of the model.|
| **Failed Model Run** | A model run is considered failed if it fails to produce an output | Useful for reporting current status of a model. A failed status has an associated failed error message describing why the run failed|


# Input Vector Creation
This sub-domain is about gathering all of the data necessary to create a ensemble input vector based on a model's requirements.

| Name | Definition | Notes |
| ----| ---- | ---- | 
| Data Source | | |
| In-Series | | |
| Out-Series | | |
| Ingestion | | |
| Interpolation | | |
| Post Processing | | |
| Data Conpleteness | | |
| Data Staleness | | |
| DSPEC | | |
| **Acquired Time** | The time at which the application aquired the data |  |


## Bullpen (to be deleted)
 A model may require data from multiple sources, and not all of them may produce an ensemble input 