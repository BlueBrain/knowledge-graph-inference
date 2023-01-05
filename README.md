<span style="color:red">TODO:</span>
- <span style="color:red">Figure out which fields are optional and which are mandatory</span>
- <span style="color:red">For some fields that are strings, it's only a valid set of strings that will work (ex: org, project)</span>


# Rule
(defined by the ontology)

       {
              "id": uri
              
              "type": "DataGeneralizationRule"
              
              "_schemaProject": "https://bbp.epfl.ch/nexus/v1/projects/bbp/inference-rules"
              
              "description": str
              
              "name": str
              
              "searchQuery": SearchQuery
              
              "premise": Premise or [Premise] *OPTIONAL*
              
              "targetResourceType": uri *OPTIONAL*
       }


# SearchQuery

- **QueryPipe**
- **Query**

## 1. QueryPipe

       {
               "type": "QueryPipe"
              
               "head": Query
              
               "rest": SearchQuery
       }

## 2. Query

<span style="color:red">(is there a difference between the sparql/es q and p? 
other than maybe "resultParameterMapping")</span>

- QueryType = "SparqlQuery" or "ElasticSearchQuery"

        {
                "type": QueryType
                
                "hasParameter": [Parameter] or Parameter
                
                "queryConfiguration": QueryConfiguration
                
                "resultParameterMapping": ParameterMapping or [ParameterMapping]
                
                "hasBody": str
        }

- QueryType = "SimilarityQuery"

        {
        
                "type": QueryType
                
                "hasParameter": [Parameter] or Parameter
                
                "queryConfiguration": QueryConfiguration
                
                "k": int
                
                "searchTargetParameter": str
        
        }

- QueryType = "ForgeSearchQuery"

       {
              "type": QueryType
       }


---
**ParameterMapping**
       
       {
               "parameterName": str
              
               "path": str
       }

**Parameter**
              
       {
              "type": ParameterType
              
              "description": str *OPTIONAL*
              
              "name": str
              
              "optional": bool *OPTIONAL*
       }
---

# QueryConfiguration

- **SearchQueryConfiguration**
- **SimilarityQueryConfiguration**

## 1. SearchQueryConfiguration

- SearchQueryConfiguration in a Query & its "type" == ""SparqlQuery"

       {
              "sparqlView": View OPTIONAL
              
              "org": str
              
              "project": str
       }

-  SearchQueryConfiguration in a Query & its "type" == "ElasticSearchQuery"

       {
              "elasticSearchView": View OPTIONAL
              
              "org": str
              
              "project": str
       }

<span style="color:red">Is there a ForgeSearchQueryConfig??</span>

## 2. SimilarityQueryConfiguration

    {
            "description" str

            "similarityView": View

            "statisticsView" View

            "boosted": bool

            "boostingView": View

            "embeddingModel": EmbeddingModel

            "org": str

            "project": str
    }
___
**View**

       {

              "id": uri

              "type": str
       
       }
<span style="color:red">TODO exhaustive list of view types (ex:    "ElasticSearchView")</span>

**EmbeddingModel**

       {
              "id": uri
              "type": "EmbeddingModel"
              "hasSelector": {
                     "type"
                     "conformsTo"
                     "value"
              }
              "org": str
              "project": str
       }
       
<span style="color:red">TODO</span>
____


# Premise
<span style="color:red">
(TODO Anything particular or same format as Query)
<br>
(TODO Any differences between different Premise Types/QueryTypes ?)
</span>

## 1. SparqlPremise

       {
              "type": "SparqlPremise"
              
              "hasBody": str
              
              "hasParameter": [Parameter] or Parameter
              
              "queryConfiguration": QueryConfiguration
       }

hasBody: the sparql query

## 2. ElasticSearchPremise

<span style="color:red">Not implemented yet??</span>


## 3. ForgeSearchPremise

<span style="color:red">TODO</span>

       {
              "targetParameter"

              "targetPath"

              "pattern"
       }


# Types

## 1. QueryType
    - "SparqlQuery"
    - "ElasticSearchQuery"
    - "SimilarityQuery"
    - "ForgeSearchQuery"

## 2. PremiseType
    - "SparqlPremise"
    - "ElasticSearchPremise"
    - "ForgeSearchPremise"

## 3. ParameterType
    - "list"
    - "uri_list"
    - "sparql_value_list"
    - "sparql_value_uri_list"
    - "uri"
    - "str"
    - "path" # TEMP
    - "MultiPredicateObjectPair" # TEMP