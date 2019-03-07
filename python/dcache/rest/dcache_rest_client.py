#!/usr/bin/env python

import io
import json
import pycurl
import os
import sys

from StringIO import StringIO

DEFAULT_HOST = "example.org"
DEFAULT_PORT = 3880

class dCacheRestClient(object):
    def __init__(self, host=None, port=None):
        self.host = host if host else DEFAULT_HOST
        self.port = port if port is not None else DEFAULT_PORT
        self.url = "https://"+self.host+":"+str(self.port) + "/"

        self.curl = pycurl.Curl()

        """
        With very few exceptions, PycURL option names are derived from
        libcurl option names by removing the CURLOPT_ prefix.
        """

        self.curl.setopt(pycurl.CAPATH,"/etc/grid-security/certificates")
	uid = os.getuid()
	proxy = "/tmp/x509up_u"+str(uid)
        self.curl.setopt(pycurl.CAINFO, proxy)
        self.curl.setopt(pycurl.SSLCERT, proxy)
        self.curl.setopt(pycurl.SSLKEY, proxy)

    def execute(self, query):
        url = self.url + query
        buffer = io.BytesIO()
        self.curl.setopt(pycurl.URL, url)
        self.curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self.curl.perform()
        rc=self.curl.getinfo(pycurl.HTTP_CODE)

        if rc != 200 :
            raise Exception("Failed to execute query %s"%(rc,))
        return json.load(StringIO(buffer.getvalue()))

    def get_file_info(self, path):
    	return self.execute("api/v1/namespace" + path)

    def get_storage_info(self, pnfsid):
    	return self.execute("api/v1/id/"+pnfsid)

    def get_file_volume(self, path):
	data = self.get_file_info(path)
	if data['fileType'] != "REGULAR":
	   return None
	pnfsid = str(data['pnfsId'])
	storage_info = self.get_storage_info(pnfsid)
	return storage_info['storageInfo']['volume']

if __name__ == "__main__":
   if len(sys.argv) < 2:
      print "provide space separated list of files"
      sys.exit(1)
   try:
	f = dCacheRestClient()
	for arg in sys.argv[1:]:
	    	volume = f.get_file_volume(arg)
		print arg, volume
   except Exception as e:
        print str(e)
        sys.exit(1)






