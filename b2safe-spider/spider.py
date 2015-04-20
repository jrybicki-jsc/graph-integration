import json
import requests
import os

base_url = os.getenv('B2SHARE_URL', 'https://b2share.eudat.eu/api/records')

creds = {
    'access_token': os.getenv('B2SHARE_TOKEN', None)
}
# 100 is the maximum value (higher values will be truncated at the server)
PAGE_SIZE = 100


def retrieve_items(page=0):
    params = dict()
    params.update(creds)
    pagination = {'page_size': PAGE_SIZE, 'page_offset': page}
    params.update(pagination)
    r = requests.get(url=base_url, params=params)
    print '%d retrieved, elapsed time %s' % (len(r.json()), r.elapsed)
    content = r.json()
    if '...' in content:
        print 'There is next page!'
        content.remove('...')
        content = content + retrieve_items(page + 1)

    return content


if __name__ == "__main__":
    if 'B2SHARE_TOKEN' not in os.environ:
        print 'To retrieve remote records, provide access token in ' \
              'B2SHARE_TOKEN environment ' \
              'variable'
        exit(-1)

    items = retrieve_items(page=0)
    fname = 'out.json'
    print 'Retrieved %d writing to %s' % (len(items), fname)
    with open(fname, 'w+') as f:
        json.dump(items, f)
