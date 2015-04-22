
import argparse
import json
import pprint
import sys
import urllib
import urllib2
import csv
from datetime import datetime
import os.path

import oauth2


API_HOST = 'api.yelp.com'
SEARCH_PATH = '/v2/search/'
BUSINESS_PATH = '/v2/business/'

# OAuth credential placeholders that must be filled in by users.
CONSUMER_KEY = None
CONSUMER_SECRET = None
TOKEN = None
TOKEN_SECRET = None


def request(host, path, url_params=None):

    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(host, urllib.quote(path.encode('utf8')))

    consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': CONSUMER_KEY
        }
    )
    token = oauth2.Token(TOKEN, TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()
    
    print u'Querying {0} ...'.format(url)

    conn = urllib2.urlopen(signed_url, None)
    response_data = conn.read()

    try:
        response = json.loads(response_data)
    finally:
        conn.close()

    return response

def search(offset): #
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    location = "San Francisco, CA"


    url_params = {
        'location': location.replace(' ', '+'),
        'limit': 20,
        'offset': offset
    } 
    results = request(API_HOST, SEARCH_PATH, url_params=url_params)
    return results['businesses']

def run_query(total_requested, offset_increment):
	offset = 0
	bizlist = []

	while offset < total_requested: 
	    try:
	        bizlist += prep_dict(search(offset))
	    except urllib2.HTTPError as error:
	        sys.exit('Encountered HTTP error {0}. Abort program.'.format(error.code))
	    offset += offset_increment

	return bizlist

def prep_dict(p):

	new_list = []

	for item in p:
		item_dict = {}
		for entry in item:
			if isinstance(item[entry], dict):
				for key, value in item[entry].iteritems():
					item_dict[key] = value.encode('utf-8') if isinstance(value, unicode) else value
			else:
				item_dict[entry] = item[entry].encode('utf-8') if isinstance(item[entry], unicode) else item[entry]
		new_list.append(item_dict)

	return new_list

def get_field_names(bizlist):

	fieldnames = []
		
	for bus in bizlist[0:20]:
		for v in bus:
			if v not in fieldnames:
				fieldnames.append(v)

	return fieldnames

bizlist = run_query(10000, 20)

file_time = datetime.now()
time_string = file_time.strftime('%Y-%m-%d_%H-%M-%S')
csvfilename = os.path.join('business_csvs','businesses-{}.csv'.format(time_string))


with open(csvfilename, 'w') as csvfile:
    fieldnames = get_field_names(bizlist)
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

    writer.writeheader()

    for row in bizlist:
    	writer.writerow(row)
    


