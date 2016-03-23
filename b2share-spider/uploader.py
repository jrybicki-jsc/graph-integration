from functools import partial
import os
from py2neo import Node, Graph, Path, GraphError
import json
from sys import exit


class DummyGraph(object):
    def __init__(self):
        super(DummyGraph, self).__init__()

    def merge_one(self, *args, **kwargs):
        print 'Dummy merging one'
        return Node()

    def create(self, *args, **kwargs):
        print 'Dummy create'

        return Node()

    def create_unique(self, *args, **kwargs):
        print 'Dummy create unique'

    def push(self, *args, **kwargs):
        print 'Dummy pushing'


def delete_constrain(graph, label, property):
    try:        
        graph.schema.drop_uniqueness_constraint(label, property)
    except GraphError as error:
        print 'Unable to delete constrain %r' % error


def clean_graph(graph):
    print 'Cleaning up...'
    delete_constrain(graph, 'Keyword', 'value')
    delete_constrain(graph, 'Person', 'email')
    delete_constrain(graph, 'Data', 'PID')
    graph.cypher.execute('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')


def get_graph(cleanup=False, dry_run=False, constrains=True):
    if dry_run:
        print 'Dry run: using dummy graph'
        return DummyGraph()

    uri = os.getenv('NEO4J_URI', 'http://neo4j:neo@localhost:7474/db/data/')
    print 'Connecting to graph at: %s' % uri
    graph = Graph(uri)
    if cleanup:
        clean_graph(graph)
    if not constrains:
        return graph
      
    try:
        graph.schema.create_uniqueness_constraint('Keyword', 'value')
        graph.schema.create_uniqueness_constraint('Person', 'email')
        graph.schema.create_uniqueness_constraint('Data', 'PID')
    except GraphError as error:
        print 'Unable to create constrains ' \
              '(they already exist perhaps?) %r' % error
        # return None

    return graph


def get_fields(record, fields):
    return {k: v.replace('\n',' ').strip() for k, v in record.iteritems() if k in fields and v
            is not None and v != ''}


def safe_get_field(field_name, record):
    return record[field_name] if field_name in record else None


def get_uploader(record):
    email = safe_get_field('uploaded_by', record)
    if email is None or email == '':
        return None
    return {'name': email, 'email': email}


def get_metadata(record):
    return safe_get_field('domain_metadata', record)


def get_keywords(record):
    return safe_get_field('keywords', record)


def get_data_object(record):
    """
    title, description, PID, publication_date
    :param record:
    :return:
    """
    fields = ['description', 'PID', 'title', 'publication_date']
    data = get_fields(record, fields)

    record_url = 'https://b2share.eudat.eu/record/'
    if 'recordID' in record:
        data['url'] = '%s%s' % (record_url, record['recordID'])

    return data


uploaded = 0
skipped = 0


def process_record(graph, record):
    """
    domain: (something like community)
    creator: (names or project)
    """
    global uploaded, skipped

    uploader = get_uploader(record)
    do = get_data_object(record)
    md = get_metadata(record)
    keywords = get_keywords(record)
    print '(%s)-[:CREATED]->(%s)-[:DESCRIBED_BY]->(%s)' % (uploader, do, md)
    for keyword in keywords:
        print '(%s)-[:HAS_TAG]->(%s)' % (do, keyword)

    if uploader is None or do is None:
        skipped += 1
        return

    p = graph.merge_one('Person', 'email', uploader['email'])
    p.set_properties(uploader)

    o = graph.merge_one('Data', 'PID', do['PID'])
    o.set_properties(do)

    graph.create_unique(Path(p, 'CREATED', o))

    if md is not None:
        m = Node.cast(md)
        m.labels.add('Metadata')
        graph.create(m)
        graph.create_unique(Path(o, 'DESCRIBED_BY', m))

    for keyword in keywords:
        keyword_tag = graph.merge_one('Keyword', 'value', keyword)
        graph.create_unique(Path(o, 'HAS_TAG', keyword_tag))

    uploaded += 1


if __name__ == "__main__":
    fname = 'out.json'
    with open(fname) as f:
        items = json.load(f)

    print 'Loaded %d records from %s' % (len(items), fname)

    g = get_graph(cleanup=False, dry_run=False, constrains=True)

    if g is None:
        exit(-1)

    map(partial(process_record, g), items)
    print 'Uploaded %s (skipped %s). Loaded %s' % (uploaded, skipped,
                                                   len(items))

    g.push()
