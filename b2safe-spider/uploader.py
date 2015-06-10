from functools import partial
import os
from py2neo import Node, Graph, Path, GraphError
import json
from sys import exit


def clean_graph(g):
    print 'Cleaning up...'
    graph.cypher.execute('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')


def get_graph(cleanup=False):
    uri = os.getenv('NEO4J_URI', 'http://neo4j:neo@localhost:7474/db/data/')
    print 'Connecting to graph at: %s' % uri
    graph = Graph(uri)
    try:
        graph.schema.create_uniqueness_constraint('Person', 'email')
        graph.schema.create_uniqueness_constraint('Data', 'PID')
    except GraphError as error:
        print 'Unable to create constrains ' \
              '(they already exist perhaps?) %r' % error
        return None

    return graph


def get_uploader(record):
    if 'uploaded_by' not in record:
        # invalid record?
        return None

    email = record['uploaded_by']
    return {'name': email, 'email': email}


def get_data_object(record):
    """
    title, description, PID, publication_date
    :param record:
    :return:
    """
    fields = ['description', 'PID', 'title', 'publication_date']
    data = dict()
    for field in fields:
        if field not in record:
            continue
        data[field] = record[field]

    record_url = 'https://b2share.eudat.eu/record/'
    if 'recordID' in record:
        data['url'] = '%s%s' % (record_url, record['recordID'])

    return data


def get_metadata(record):
    # TODO: 'domain_metadat' needs to be flatten somehow
    fields = ['keywords']
    md = dict()
    for field in fields:
        if field not in record:
            continue
        md[field] = record[field]

    return md


def process_record(graph, record):
    """
    domain: (something like community)
    creator: (names or project)
    """
    uploader = get_uploader(record)
    do = get_data_object(record)
    md = get_metadata(record)
    print '(%s)-[:CREATED]->(%s)-[:DESCRIBED_BY]->(%s)' % (uploader, do, md)

    if uploader is None:
        return
    p = graph.merge_one('Person', 'email', uploader['email'])
    p.set_properties(uploader)

    o = graph.merge_one('Data', 'PID', do['PID'])
    o.set_properties(do)

    m = Node.cast(md)
    m.labels.add('Metadata')
    graph.create(m)

    graph.create_unique(Path(p, 'CREATED', o, 'DESCRIBED_BY', m))


if __name__ == "__main__":
    fname = 'out.json'
    with open(fname) as f:
        items = json.load(f)

    print 'Loaded %d records from %s' % (len(items), fname)
    g = get_graph()
    if g is None:
        exit(-1)

    map(partial(process_record, g), items)
    g.push()
