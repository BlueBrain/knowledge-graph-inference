BBP inference 
**************


### Similarity Model:

Steps in creation and persistence of a new model:

- Create an implementation of **Model** (`similarity_model/building/model`) <br>
Examples in `similarity_model/building/model_impl/...`

- Optionally, if the data necessary to run the model isn't loaded by the model itself create an
implementation of **ModelData** (`similarity_model/building/model_data`) <br>
Example in `similarity_model/building/model_data_impl/...`

- Create a **ModelDescription** instance (`similarity_model/building/model_description`). This instance
needs to contain all the information necessary by the model registration pipeline, including the
class of the **Model** implementation.  <br> Examples found along model implementation examples: 
`similarity_model/building/model_impl/...`


- Use the **ModelRegistrationPipeline** to run either all steps of the pipeline or a specific step.
In order to run the pipeline the following parameters are necessary:
    - A list of models to run and register: this is specified through a **ModelDescription**
      instance and a revision. The revision is optional. 
    - A bucket configuration which specifies in which bucket registration will happen.
    - Optionally, a **ModelData** implementation instance if the model specified through the model
description needs it

Example in `similarity_model/run.py`
