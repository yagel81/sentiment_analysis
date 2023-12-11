
"""
This script retrieves articles from The Guardian API for a specified date, stores them locally, and organizes them
into individual JSON files for each date.

Requirements:
- requests
- json
- os
- datetime
"""


import json
import requests
from os import makedirs
from os.path import join, exists
from datetime import date, timedelta
import datetime

# ARTICLES_DIR = join('tempdata', 'articles')
ARTICLES_DIR = '/tmp/staging/project/api_calls/'
makedirs(ARTICLES_DIR, exist_ok=True)

MY_API_KEY = '<MY_API_KEY>'

# MY_API_KEY = open("creds_guardian.txt").read().strip()
API_ENDPOINT = 'http://content.guardianapis.com/search'
my_params = {
    'from-date': "",
    'to-date': "",
    'order-by': "newest",
    'show-fields': 'all',
    'page-size': 200,
    'api-key': MY_API_KEY
}
wanted_date = str(datetime.date.today())
# '2022-04-11'

# day iteration from here:
# http://stackoverflow.com/questions/7274267/print-all-day-dates-between-two-dates
start_date = datetime.datetime.strptime(wanted_date, '%Y-%m-%d').date()
end_date = datetime.datetime.strptime(wanted_date, '%Y-%m-%d').date()
# start_date = date(2022, 4, 10)
# end_date = date(2022, 4, 10)
dayrange = range((end_date - start_date).days + 1)
for daycount in dayrange:
    dt = start_date + timedelta(days=daycount)
    datestr = dt.strftime('%Y-%m-%d')
    fname = join(ARTICLES_DIR, datestr + '.json')
    if not exists(fname):
        # then let's download it
        print("Downloading", datestr)
        all_results = []
        my_params['from-date'] = datestr
        my_params['to-date'] = datestr
        current_page = 1
        total_pages = 1
        while current_page <= total_pages:
            print("...page", current_page)
            my_params['page'] = current_page
            resp = requests.get(API_ENDPOINT, my_params)
            data = resp.json()
            all_results.extend(data['response']['results'])
            # if there is more than one page
            current_page += 1
            total_pages = data['response']['pages']

        with open(fname, 'w') as f:
            print("Writing to", fname)

            # re-serialize it for pretty indentation
            f.write(json.dumps(all_results))

"""
This script fetches articles from The Guardian API for a specified date, saves them locally, and organizes them into
individual JSON files for each date.
"""
