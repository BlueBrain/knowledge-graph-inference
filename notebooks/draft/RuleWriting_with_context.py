import getpass
import uuid

from kgforge.core import KnowledgeGraphForge

from inference_tools.utils import (check_premises,
                                   execute_query,
                                   execute_query_pipe,
                                   apply_rule,
                                   get_rule_parameters)

from inference_tools.type import ParameterType, QueryType

LOCAL_TOKEN = True
TOKEN_PATH = 'token.txt'
CONFIG_PATH = "config.yaml"


def allocate_forge_session(org, project):
    ENDPOINT = "https://bbp.epfl.ch/nexus/v1"

    if LOCAL_TOKEN:
        with open(TOKEN_PATH) as f:
            TOKEN = f.read()
    else:
        TOKEN = getpass.getpass()

    searchendpoints = {
        "sparql": {
            "endpoint": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
        }
    }

    return KnowledgeGraphForge(
        CONFIG_PATH,
        endpoint=ENDPOINT,
        token=TOKEN,
        bucket=f"{org}/{project}",
        searchendpoints=searchendpoints,
        debug=True
    )




rule_id3 = f"https://bbp.epfl.ch/neurosciencegraph/data/{uuid.uuid4()}"


fq3 = """
    SELECT ?id

    WHERE {    
        BIND(IF($SearchDown, ?id, ?GeneralizedFieldValue) AS ?contains)
        BIND(IF($SearchDown, ?GeneralizedFieldValue, ?id) AS ?contained)
        ?id rdfs:subClassOf $GeneralizedFieldName .
        ?contained schema:isPartOf* ?contains  .
    } LIMIT 5000
"""

fqo3 = {
    "type": QueryType.SPARQL_QUERY.value,
    "hasBody": fq3,
    "hasParameter": [
        {
            "type": ParameterType.PATH.value,
            "description": "Field Name being generalized",
            "name": "GeneralizedFieldName"
        },

        {
            "type": ParameterType.URI.value,
            "description": "Field Value being generalized",
            "name": "GeneralizedFieldValue"
        },

        {
            "type": ParameterType.PATH.value,
            "description": "Whether we are searching descendants or parents",
            "name": "SearchDown",
        },
    ],
    "queryConfiguration": {
        "sparlqlView": {
            "id": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
        },

        "org": "neurosciencegraph",
        "project": "datamodels"
    },
    "resultParameterMapping": [
        {
            "parameterName": "all_values",
            "path": "id"
        },
    ]
}

sq3 = """
    SELECT ?id ?br
    WHERE { 
        ?id rdf:type $TypeQueryParameter .
        ?id $PathToGeneralizedField ?value .
        ?id <https://bluebrain.github.io/nexus/vocabulary/deprecated> ?_deprecated .
        ?id $UserContext .
        Filter (?_deprecated = 'false'^^xsd:boolean && ?value in $all_values)
    } LIMIT 5000
"""

sqo3 = {
    "type": "SparqlQuery",
    "hasBody": sq3,
    "hasParameter": [
        {
            "type": ParameterType.SPARQL_LIST.value,
            "description": "All valid values for the generalized field",
            "name": "all_values"
        },
        {
            "type": ParameterType.PATH.value,
            "description": "type of the queried entity",
            "name": "TypeQueryParameter"
        },
        {
            "type": ParameterType.PATH.value,
            "description": "Path to Generalized Field",
            "name": "PathToGeneralizedField"
        },

        {
            "type": ParameterType.MUTLI_PREDICATE_OBJECT_PAIR.value,
            "description": "Whether we are searching descendants or parents",
            "name": "UserContext"
        }
    ],
    "queryConfiguration": {
        "sparlqlView": {
            "id": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
        },
        "org": "bbp",
        "project": "atlas"
    },
    "resultParameterMapping": {
        "parameterName": "test",
        "path": "entity"
    }
}

my_rule2 = {
    "id": rule_id3,
    "type": "DataGeneralizationRule",
    "_schemaProject": "https://bbp.epfl.ch/nexus/v1/projects/bbp/inference-rules",
    "description": """
        With Context - Given an entity type (e.g NeuronMorphology, Trace, cell type, ...) 
        linked to an entity by a provided path, generalise to entities associated 
        with the descendants or parents of the entity
    """,
    "name": """
        With Context - Generalise up (ancestors) and/or down (descendants) a (combination of) 
        "hierarchy in a BBP ontology (e.g. cell type, brain region)
    """,
    "searchQuery": {
        "type": "QueryPipe",
        "head": fqo3,
        "rest": sqo3
    },
    "targetResourceType": "Entity"
}

input_filters = {

    "TypeQueryParameter": "NeuronMorphology",
    "GeneralizedFieldName": "BrainRegion",
    "GeneralizedFieldValue": "<http://api.brain-map.org/api/v2/data/Structure/315>",
    "PathToGeneralizedField": "nsg:brainLocation/nsg:brainRegion",
    "SearchDown": "true",
    "UserContext": [
        (
            ("rdf:type", "path"),
            ("NeuronMorphology", "path")
        ),  # redundant with the query content but just to show an example of multiple predicate object pairs
        (
               ("contribution/agent", "path"),
               ("<https://bbp.epfl.ch/neurosciencegraph/data/7c47aa15-9fc6-42ec-9871-d233c9c29028>", "path")
        )
    ]

}
# res3 = apply_rule(allocate_forge_session, my_rule2, input_filters, debug=True)
#
# print(len(res3))


# rule_forge = allocate_forge_session("bbp", "inference-rules")
#
# rule_forge.register(rule_forge.from_json(my_rule2))
#
# new_rule = rule_forge.retrieve(rule_id3)
#
# rule_forge.tag(new_rule, "v1")

