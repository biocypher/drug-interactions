import random
import string
import hashlib
import pandas as pd
from enum import Enum, auto
from itertools import chain
from typing import Optional
from biocypher._logger import logger

logger.debug(f"Loading module {__name__}.")

# | File name | Content | Size |
# | --- | --- | --- |
# | ddinter_downloads_code_A.csv | Interaction involving alimentary tract and metabolism drugs | 3.3MB |
# | ddinter_downloads_code_B.csv | Interaction involving blood and blood forming organs drugs | 867.7KB |
# | ddinter_downloads_code_D.csv | Interaction involving dermatologicals drugs | 1.5MB |
# | ddinter_downloads_code_H.csv | Interaction involving systemic hormonal preparations, excluding sex hormones and insulins drugs | 705.1KB |
# | ddinter_downloads_code_L.csv | Interaction involving antineoplastic and immunomodulating agents drugs | 3.9MB |
# | ddinter_downloads_code_P.csv | Interaction involving antiparasitic products, insecticides and repellents drugs | 317.5KB |
# | ddinter_downloads_code_R.csv | Interaction involving respiratory system drugs | 1.8MB |
# | ddinter_downloads_code_V.csv | Interaction involving various drugs | 700.8KB |


class DDInterAdapterNodeType(Enum):
    """
    Define types of nodes the adapter can provide.
    """

    DRUG = auto()


class DDInterAdapterDrugField(Enum):
    """
    Define possible fields the adapter can provide for drugs.
    """

    ID = "DDInterID"
    NAME = "Drug"


class DDInterAdapterEdgeType(Enum):
    """
    Enum for the types of edges in the adapter.
    """

    DRUG_DRUG_INTERACTION = auto()


class DDInterAdapterDrugDrugEdgeField(Enum):
    """
    Define possible fields the adapter can provide for drug-drug edges.
    """

    INTERACTION_TYPE = auto()
    INTERACTION_LEVEL = "Level"
    CLASS = auto()


class DDInterAdapter:
    """
    BioCypher adapter for DDInter data. Generates nodes and edges for creating a
    knowledge graph.

    Args:
        node_types: List of node types to include in the result.
        node_fields: List of node fields to include in the result.
        edge_types: List of edge types to include in the result.
        edge_fields: List of edge fields to include in the result.
    """

    def __init__(
        self,
        data_file_paths: list,
        node_types: Optional[list] = None,
        node_fields: Optional[list] = None,
        edge_types: Optional[list] = None,
        edge_fields: Optional[list] = None,
    ):
        self._set_types_and_fields(node_types, node_fields, edge_types, edge_fields)
        self._preprocess(data_file_paths)

    def _preprocess(self, data_file_paths):
        """
        Preprocess the data files to generate nodes and edges. Open each csv
        with Pandas, aggregate drug IDs and names, and store them in the `nodes`
        attribute. At the same time, extract drug-drug interactions and store
        them in the `edges` attribute.
        """
        logger.info("Preprocessing data.")

        self.nodes = pd.DataFrame()
        self.edges = pd.DataFrame()

        for file_path in data_file_paths:
            # Read the csv file
            df = pd.read_csv(file_path)

            # Interaction class: end of file name
            interaction_class = file_path.split("_")[-1].split(".")[0]
            interaction_class_dict = {
                "A": "Alimentary tract and metabolism",
                "B": "Blood and blood forming organs",
                "D": "Dermatologicals",
                "H": "Systemic hormonal preparations",
                "L": "Antineoplastic and immunomodulating agents",
                "P": "Antiparasitic products",
                "R": "Respiratory system",
                "V": "Various",
            }

            # Nodes: Aggregate DDInterID_A, DDInterID_B, and Drug_A, Drug_B
            # columns in a new dataframe with columns DDInterID and Drug
            df1 = df[["DDInterID_A", "Drug_A"]].rename(
                columns={"DDInterID_A": "DDInterID", "Drug_A": "Drug"}
            )
            df2 = df[["DDInterID_B", "Drug_B"]].rename(
                columns={"DDInterID_B": "DDInterID", "Drug_B": "Drug"}
            )
            nodes = pd.concat([df1, df2])

            # Edges: Extract DDInterID_A, DDInterID_B, and Level columns
            edges = df[["DDInterID_A", "DDInterID_B", "Level"]].copy()

            # Add interaction class to edges
            edges["Class"] = interaction_class_dict[interaction_class]
            edges

            # Concatenate to the main nodes and edges dataframes
            self.nodes = pd.concat([self.nodes, nodes])
            self.edges = pd.concat([self.edges, edges])

        # Remove duplicates
        self.nodes = self.nodes.drop_duplicates()

    def get_nodes(self):
        """
        Returns a generator of node tuples for node types specified in the
        adapter constructor.
        """

        logger.info("Generating nodes.")

        if DDInterAdapterNodeType.DRUG in self.node_types:
            for index, row in self.nodes.iterrows():
                yield (
                    row["DDInterID"],
                    "drug",
                    {
                        "name": row["Drug"].replace("'", ""),
                    },
                )

    def get_edges(self):
        """
        Returns a generator of edge tuples for edge types specified in the
        adapter constructor.

        Args:
            probability: Probability of generating an edge between two nodes.
        """

        logger.info("Generating edges.")

        for index, row in self.edges.iterrows():
            # create md5 hash for edge id from entire row using hashlib
            edge_id = hashlib.md5(str(row).encode()).hexdigest()

            yield (
                edge_id,
                row["DDInterID_A"],
                row["DDInterID_B"],
                "drug_drug_interaction",
                {
                    "level": row["Level"],
                    "class": row["Class"],
                },
            )

    def get_node_count(self):
        """
        Returns the number of nodes generated by the adapter.
        """
        return len(list(self.get_nodes()))

    def _set_types_and_fields(self, node_types, node_fields, edge_types, edge_fields):
        if node_types:
            self.node_types = node_types
        else:
            self.node_types = [type for type in DDInterAdapterNodeType]

        if node_fields:
            self.node_fields = node_fields
        else:
            self.node_fields = [field for field in DDInterAdapterDrugField]

        if edge_types:
            self.edge_types = edge_types
        else:
            self.edge_types = [type for type in DDInterAdapterEdgeType]

        if edge_fields:
            self.edge_fields = edge_fields
        else:
            self.edge_fields = [field for field in chain()]
