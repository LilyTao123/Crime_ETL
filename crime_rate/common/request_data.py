import pandas as pd
import requests
import json

# If date is None, will return the lastest crime data
class Request_Data():
    def __init__(self, url_orig):
        self.orgi = url_orig

    def get_url_specific(self, latlong, date=None):
        urls = []
        if date == None:
            for i in range(len(latlong)):
                url_p = self.orgi + 'lat='+ str(latlong.loc[i,'lat']) + '&'+ 'lng='+ str(latlong.loc[i,'long'])
                urls.append(url_p)
            return urls
        for i in range(len(latlong)):
            url_p = self.orgi + 'lat='+ str(latlong.loc[i,'lat']) + '&'+ 'lng='+ str(latlong.loc[i,'long']) +'&'+'date='+date
            urls.append(url_p)
        return urls

    def get_url_custom (self, latlong, date = None):
        post = ''
        for i in range(len(latlong)):
            p = str(latlong.loc[i,'lat']) + ',' + str(latlong.loc[i,'long'])
            if i < len(latlong)-1:
                p = p + ':'
            post = post + p
        url = self.orgi + 'poly=' + post + '&'+ 'date='+date
        return url

    @staticmethod
    def data_from_url(url):
        r = requests.post(url)
        print(r.status_code)
        a = json.loads(r.text)
        res = pd.json_normalize(a)
        df = pd.DataFrame(res)
        return df

    