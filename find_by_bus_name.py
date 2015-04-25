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
with open('keys.json', 'rb') as key_file:
	KEYS = json.load(key_file)

CONSUMER_KEY = str(KEYS['CONSUMER_KEY'])
CONSUMER_SECRET = str(KEYS['CONSUMER_SECRET'])
TOKEN = str(KEYS['TOKEN'])
TOKEN_SECRET = str(KEYS['TOKEN_SECRET'])



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

def search(dbaname): #

    location = "San Francisco CA"
    term = dbaname.lower()


    url_params = {
    	'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': 20
        #'offset': offset,
        #'bounds':'37.767794,-122.431304|37.765734,-122.427735',
        ##'radius_filter':'100',
        ##'sort':'2'
    } 
    results = request(API_HOST, SEARCH_PATH, url_params=url_params)
    return results['businesses']

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


def main():
	file_time = datetime.now()
	time_string = file_time.strftime('%Y-%m-%d_%H-%M-%S')
	destfilename = os.path.join('by_name_csvs','by_name-{}.csv'.format(time_string))
	lookupfilename = os.path.join('20150422_Registered_Business_Locations - Closed_2.csv')

	with open(destfilename, 'wb') as businesses_found, open(lookupfilename, 'rU') as lookupfile:
		first_batch = True
		writer = None

		reader = csv.DictReader(lookupfile)

		for item in reader:
		#while offset < number_to_fetch:
			location_id = item['Location_ID']
			dba_name = item['DBA Name']
			print location_id, dba_name

			bizlist = prep_dict(search(dba_name))
			if bizlist:
				if first_batch:
					fieldnames = ['Location_ID_sfdata'] + ['DBA Name sfdata'] + get_field_names(bizlist)
					writer = csv.DictWriter(businesses_found, fieldnames=fieldnames, extrasaction='ignore')
					writer.writeheader()
					first_batch=False
				for row in bizlist:
					row['DBA Name sfdata'] = dba_name
					row['Location_ID_sfdata'] = location_id
					writer.writerow(row)
				#offset += offset_increment


if __name__ == "__main__":
	main()

