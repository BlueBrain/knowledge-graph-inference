"""Test resource payloads."""

# ----------------------- Brain region classes -------------
brain_region = {
    "@context": "https://neuroshapes.org",
    "@id": "https://neuroshapes.org/BrainRegion",
    "@type": "Class",
    "altLabel": "regional part of the brain",
    "definition": "Anatomical divisons of the brain according to one or more criteria, e.g. cytoarchitectural, gross anatomy. Parts may be contiguous in 3D or not, e.g., basal ganglia.",
    "isDefinedBy": "https://bbp.epfl.ch/ontologies/core/bmo",
    "label": "Brain Region",
}
isocortex = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/315",
    "@type": "Class",
    "atlas_id": 746,
    "color_hex_triplet": "70FF71",
    "graph_order": 5,
    "hemisphere_id": 3,
    "identifier": "315",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "mba:695",
    "label": "Isocortex",
    "notation": "Isocortex",
    "prefLabel": "Isocortex",
    "st_level": 5,
    "subClassOf": "https://neuroshapes.org/BrainRegion"
}

ILA = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/44",
    "@type": "Class",
    "atlas_id": 146,
    "color_hex_triplet": "59B363",
    "graph_order": 245,
    "hemisphere_id": 3,
    "identifier": "44",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "http://api.brain-map.org/api/v2/data/Structure/315",
    "label": "Infralimbic area",
    "notation": "ILA",
    "prefLabel": "Infralimbic area",
    "st_level": 8,
    "subClassOf": "nsg:BrainRegion"
}

PTLp = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/22",
    "@type": "Class",
    "atlas_id": 285,
    "color_hex_triplet": "009FAC",
    "graph_order": 339,
    "hemisphere_id": 3,
    "identifier": "22",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "http://api.brain-map.org/api/v2/data/Structure/315",
    "label": "Posterior parietal association areas",
    "notation": "PTLp",
    "prefLabel": "Posterior parietal association areas",
    "st_level": 6,
    "subClassOf": "nsg:BrainRegion"
}

ACA = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/31",
    "@type": "Class",
    "atlas_id": 3,
    "color_hex_triplet": "40A666",
    "graph_order": 220,
    "hemisphere_id": 3,
    "identifier": "31",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "http://api.brain-map.org/api/v2/data/Structure/315",
    "label": "Anterior cingulate area",
    "notation": "ACA",
    "prefLabel": "Anterior cingulate area",
    "st_level": 8,
    "subClassOf": "nsg:BrainRegion"
}

ACAv = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/48",
    "@type": "Class",
    "atlas_id": 5,
    "color_hex_triplet": "40A666",
    "graph_order": 232,
    "hemisphere_id": 3,
    "identifier": "48",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "http://api.brain-map.org/api/v2/data/Structure/31",
    "label": "Anterior cingulate area, ventral part",
    "notation": "ACAv",
    "prefLabel": "Anterior cingulate area, ventral part",
    "st_level": 9,
    "subClassOf": "nsg:BrainRegion"
}

thalamus = {
    "@id": "http://api.brain-map.org/api/v2/data/Structure/549",
    "@type": "Class",
    "atlas_id": 351,
    "color_hex_triplet": "FF7080",
    "graph_order": 641,
    "hemisphere_id": 3,
    "identifier": "549",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
    "isPartOf": "mba:1129",
    "label": "Thalamus",
    "notation": "TH",
    "prefLabel": "Thalamus",
    "st_level": 5,
    "subClassOf": "nsg:BrainRegion"
}

BRAIN_REGIONS = [brain_region, thalamus, isocortex, ILA, PTLp, ACA, ACAv]

# ----------------------------- MType classes --------------------------

mtype = {
    "@id": "https://neuroshapes.org/MType",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "Neuron Morphological Type",
}

pc = {
    "@id": "https://neuroshapes.org/PyramidalNeuron",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "Pyramidal neuron",
    "notation": "Pyr",
    "prefLabel": "Pyramidal neuron",
    "subClassOf": "https://neuroshapes.org/MType"
}

tpc = {
    "@id": "https://neuroshapes.org/TufterdPyramidalNeuron",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "Tufterd Pyramidal Neuron",
    "notation": "TPyr",
    "prefLabel": "Tufterd Pyramidal Neuron",
    "subClassOf": "https://neuroshapes.org/PyramidalNeuron"
}

upc = {
    "@id": "https://neuroshapes.org/UntufterdPyramidalNeuron",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "Untufterd Pyramidal Neuron",
    "notation": "UPyr",
    "prefLabel": "Untufterd Pyramidal Neuron",
    "subClassOf": "https://neuroshapes.org/PyramidalNeuron"
}

l5_upc = {
    "@id": "http://uri.interlex.org/base/ilx_0381371",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "L5_UPC",
    "notation": "L5_UPC",
    "prefLabel": "Layer 5 Untufted Pyramidal Cell",
    "subClassOf": "https://neuroshapes.org/UntufterdPyramidalNeuron"
}

l6_upc = {
    "@id": "http://uri.interlex.org/base/ilx_0381377",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "L6_UPC",
    "notation": "L6_UPC",
    "prefLabel": "Layer 6 Untufted Pyramidal Cell",
    "subClassOf": "https://neuroshapes.org/UntufterdPyramidalNeuron"
}


mc = {
    "@id": "https://neuroshapes.org/MartinottiCell",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "Martinotti Cell",
    "notation": "MC",
    "prefLabel": "Martinotti Cell",
    "subClassOf": "https://neuroshapes.org/MType"
}

vpl_in = {
    "@id": "http://uri.interlex.org/base/ilx_0738235",
    "@type": "Class",
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mtypes",
    "label": "VPL_IN",
    "notation": "VPL_IN",
    "prefLabel": "Ventral posterolateral nucleus of the thalamus interneuron",
    "subClassOf": "https://neuroshapes.org/MType"
}

MTYPES = [mtype, pc, mc, upc, tpc, l5_upc, l6_upc]


# ------------------------------------ Species -------------------------------

species = {
    "@id": "https://neuroshapes.org/Species",
    "@type": "Class",
    "http://purl.obolibrary.org/obo/ncbitaxon#has_rank": {
        "@id": "http://purl.obolibrary.org/obo/NCBITaxon_species"
    },
    "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/speciestaxonomy/",
    "label": "Species",
    "subClassOf": "prov:Entity"
}

mus = {
    "@id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
    "@type": "Class",
    "isDefinedBy": "https://bbp.epfl.ch/ontologies/core/molecular-systems",
    "label": "Mus musculus",
    "subClassOf": "nsg:Species"
}


# ------------------------ Traces and morphologies ----------------------------

trace_config = [
    (
        "http://uri.interlex.org/base/ilx_0381371", "L5_UPC",
        "http://api.brain-map.org/api/v2/data/Structure/48", "ACAv",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090", "Mus musculus"
    ),
    (
        "http://uri.interlex.org/base/ilx_0381377", "L6_UPC",
        "http://api.brain-map.org/api/v2/data/Structure/44", "ILA",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090", "Mus musculus"
    ),
    (
        "https://neuroshapes.org/TufterdPyramidalNeuron", "TPC",
        "http://api.brain-map.org/api/v2/data/Structure/22", "PTLp",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090", "Mus musculus"
    ),
    (
        "http://uri.interlex.org/base/ilx_0738235", "VPL_IN",
        "http://api.brain-map.org/api/v2/data/Structure/549", "Thalamus",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090", "Mus musculus"
    ),
    (
        "https://neuroshapes.org/MartinottiCell", "Martinotti Cell",
        "http://api.brain-map.org/api/v2/data/Structure/22", "PTLp",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090", "Mus musculus"
    )
]

TRACES = []
for i, (
        mtype_id, mtype_label, region_id,
        region_label, species_id, species_label) in enumerate(trace_config):
    trace = {
        "@id": f"https://bbp.epfl.ch/neurosciencegraph/data/Trace{i + 1}",
        "@type": [
            "Trace",
            "Dataset",
            "Entity"
        ],
        "annotation": {
            "@type": [
                "Annotation",
                "MType:Annotation"
            ],
            "hasBody":  {
                "@id": mtype_id,
                "@type": [
                    "MType",
                    "AnnotationBody"
                ],
                "label": mtype_label
            },
            "name": "M-type Annotation"
        },
        "brainLocation": {
            "@type": "BrainLocation",
            "brainRegion": {
                "@id": region_id,
                "label": region_label
            }
        },
        "subject": {
            "@type": "Subject",
            "species": {
                "@id": species_id,
                "label": species_label
            }
        }
    }
    TRACES.append(trace)


MORPHOLOGIES = []
for i, (
        mtype_id, mtype_label, region_id,
        region_label, species_id, species_label) in enumerate(trace_config):
    morph = {
        "@type": [
            "NeuronMorphology",
            "Dataset",
            "Entity"
        ],
        "annotation": {
            "@type": [
                "Annotation",
                "MType:Annotation"
            ],
            "hasBody": {
                "@id": mtype_id,
                "@type": [
                    "MType",
                    "AnnotationBody"
                ],
                "label": mtype_label
            },
            "name": "M-type Annotation"
        },
        "brainLocation": {
            "@type": "BrainLocation",
            "brainRegion": {
                "@id": region_id,
                "label": region_label
            }
        },
        "subject": {
            "@type": "Subject",
            "species": {
                "@id": species_id,
                "label": species_label
            }
        }
    }
    MORPHOLOGIES.append(morph)
