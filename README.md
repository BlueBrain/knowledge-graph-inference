BBP inference 
**************

**Rule**: (defined by the ontology)

    {
        "id": **uri**

        "type": "DataGeneralizationRule"

        "_schemaProject": "https://bbp.epfl.ch/nexus/v1/projects/bbp/inference-rules",

        "description": **str**

        "name": **str**

        "searchQuery": **SearchQuery**

        "premise": **Premise** or [**Premise**] *OPTIONAL*

        "targetResourceType": **uri** *OPTIONAL*

    }




**SearchQuery** :

- **QueryPipe**
- **Query**

1. **QueryPipe**:

    {
        "type": "QueryPipe"

        "head": **Query**

        "rest": **SearchQuery**
    }

2. **Query**:

    {
        "type": **QueryType**

        "resultParameterMapping": **ParameterMapping** or [**ParameterMapping**]

        "hasBody": **str**

        "queryConfiguration: **QueryConfiguration**
    }

TODO this is closer to the Sparql/Elastic queries.
SimilarityQuery is much different. This also applies to the QueryConfiguration
which is similar between Sparql and Elastic but much more different for Similarity.

**ParameterMapping**

    {
        "parameterName": **str**

        "path": **str**
    }

**Parameter**

    {
            "type": **ParameterType**

            "description": **str** *OPTIONAL*

            "name": **str**

            "optional": **bool** *OPTIONAL*
    }

**QueryConfiguration**

    {
        "sparqlView" or "elasticSearchView": {
        "id": **uri**
        } OPTIONAL

        "org": "neurosciencegraph"

        "project": "datamodels"
    }


# **Premise**

(TODO Anything particular or same format as Query)
(TODO Any differences between different Premise Types/QueryTypes ?)

1. SparqlPremise

    {
        "type": "SparqlPremise",

        "hasBody": str (the sparql query)

        "hasParameter": [**Parameter**] or **Parameter**

        "queryConfiguration": **QueryConfiguration**
    }

2. ElasticSearchPremise


3. ForgeSearchPremise



Types

1. **QueryType**:
    - "SparqlQuery"
    - "ElasticSearchQuery"
    - "SimilarityQuery"
    - "ForgeSearchQuery"

2. **PremiseType**
    - "SparqlPremise"
    - "ElasticSearchPremise"
    - "ForgeSearchPremise"

3. **ParameterType**
    - "list"
    - "uri_list"
    - "sparql_value_list"
    - "sparql_value_uri_list"
    - "uri"
    - "str"
    - "path" # TEMP
    - "MultiPredicateObjectPair" # TEMP