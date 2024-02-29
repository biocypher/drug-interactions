from biocypher import BioCypher, Resource
from drug_interactions.adapters.ddinter_adapter import DDInterAdapter

bc = BioCypher()

urls = [
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_A.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_B.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_D.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_H.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_L.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_P.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_R.csv",
    "http://ddinter.scbdd.com/static/media/download/ddinter_downloads_code_V.csv",
]

resource = Resource(
    name="DDInter",
    url_s=urls,
    lifetime=14,
)

paths = bc.download(resource)
print(paths)

adapter = DDInterAdapter(data_file_paths=paths)

bc.write_nodes(adapter.get_nodes())
bc.write_edges(adapter.get_edges())
bc.write_schema_info(as_node=True)

bc.write_import_call()
bc.summary()
