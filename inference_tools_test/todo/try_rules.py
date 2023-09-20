import os

from kgforge.core import KnowledgeGraphForge

from inference_tools.execution import apply_rule
import cProfile
import pstats
from pstats import SortKey
import yaml
import urllib


def _get_test_forge(org, project):
    es_rule_view = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/rule_view_no_tag"
    sparql_rule_view = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/rule_view_no_tag"

    bucket = f"{org}/{project}"
    endpoint = "https://bbp.epfl.ch/nexus/v1"

    token_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "./token.txt")

    with open(token_file_path, encoding="utf-8") as f:
        token = f.read()

    config = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

    with urllib.request.urlopen(config) as e:
        conf = yaml.safe_load(e)
        if bucket == "neurosciencegraph/datamodels":
            conf["Model"]["context"]["iri"] = 'https://neuroshapes.org'

    args = dict(
        configuration=conf,
        endpoint=endpoint,
        token=token,
        bucket=bucket,
        debug=False,
        searchendpoints={
            "elastic": {"endpoint": es_rule_view},
            "sparql": {"endpoint": sparql_rule_view}
        }
    )
    return KnowledgeGraphForge(**args)


all_aspect = "https://bbp.epfl.ch/neurosciencegraph/data/abb1949e-dc16-4719-b43b-ff88dabc4cb8"

sample_neurom_seu = \
    'https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/8b2abe4c-190f-4595-9b1d-15256ca877f6'
sample_neurom_public = \
    "https://bbp.epfl.ch/neurosciencegraph/data/43edd8bf-5dfe-45cd-b6d8-1a604dd6beca"

# select_models = [
#     "Axon_co-projection-based_embedding",
#     "Brain_region_ontology-based_embedding",
#     "Coordinates-based_embedding",
#     "Dendrite_co-projection-based_embedding",
#     "Neurite_feature-based_embedding",
#     "Unscaled_TMD-based_embedding"
# ]

attempts = [
    {
        "id": all_aspect,
        "parameters": {
            'TargetResourceParameter': sample_neurom_seu,
            'SelectModelsParameter': ["Brain_region_BBP_ontology-based_embedding_-_BMO_File"],
            'LimitQueryParameter': 20
        }
    },
    {
        "id": "https://bbp.epfl.ch/neurosciencegraph/data/5d04995a-6220-4e82-b847-8c3a87030e0b",
        "parameters": {
            "GeneralizedFieldValue": "http://api.brain-map.org/api/v2/data/Structure/315",
            "GeneralizedFieldName": "BrainRegion",
            "TypeQueryParameter": "https://neuroshapes.org/Trace",
            "LimitQueryParameter": 10,
            "HierarchyRelationship": "SubclassOf",
            "SearchDirectionBlock": "Down",
            "PathToGeneralizedField": "BrainRegion"
        }
    },
    {
        "id": "https://bbp.epfl.ch/neurosciencegraph/data/9d64dc0d-07d1-4624-b409-cdc47ccda212",
        "parameters": {
            "BrainRegionQueryParameter": "http://api.brain-map.org/api/v2/data/Structure/375",
            "TypeQueryParameter": "https://neuroshapes.org/NeuronMorphology"
        }
    }
]

rule_forge = _get_test_forge("bbp", "inference-rules")

with cProfile.Profile() as pr:
    for ai in attempts:
        my_rule = rule_forge.as_json(rule_forge.retrieve(ai["id"]))

        res = apply_rule(
            _get_test_forge, my_rule, parameter_values=ai["parameters"],
            premise_check=False, debug=False
        )

        print(len(res))

    pstats.Stats(pr).sort_stats(SortKey.CUMULATIVE).print_stats(10)
