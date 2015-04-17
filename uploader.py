from py2neo import Node, Relationship, Graph, Path
import json


def get_graph():
    g = Graph('http://neo4j:neo@0.0.0.0:49154/db/data/')
    # g.schema.create_uniqueness_constraint('Person', 'email')
    # g.schema.create_uniqueness_constraint('Data', 'PID')
    return g


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
    do = dict()
    for field in fields:
        if field not in record:
            continue
        do[field] = record[field]

    record_url = 'https://b2share.eudat.eu/record/'
    if 'recordID' in record:
        do['url'] = '%s%s' % (record_url, record['recordID'])

    return do


def get_metadata(record):
    fields = ['keywords'] # 'domain_metadata',
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

    p = Node.cast(uploader)
    if p is None:
        return

    p.labels.add('Person')

    o = Node.cast(do)
    o.labels.add('Data')

    m = Node.cast(md)
    m.labels.add('Metadata')


    p_created_o = Relationship(p, 'CREATED', o)
    m_describes_o = Relationship(m, 'DESCRIBES', o)
    p = graph.merge_one(p)
    o = graph.merge_one(o)
    m = graph.merge_one(m)
    graph.create_unique(Path(p, 'CREATED', o, 'DESCRIBED_BY', m))



if __name__ == "__main__":
    fname = 'out.json'
    with open(fname) as f:
        items = json.load(f)

    print 'Loaded %d records from %s' % (len(items), fname)
    g = get_graph()
    g.cypher.execute('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')
    processor = lambda x: process_record(g, x)
    map(processor, items)

