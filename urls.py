#!/usr/bin/python

import os
import urllib2

def exitFunction(msg):
  print (msg) 
  exit (1)
  return

HOST_TO_VERIFY="127.0.0.1" #"10.35.22.67" # replace with real ip
URILLIST_FILE = '/tmp/urls.log' 
URL_PREFIX='http://'+HOST_TO_VERIFY


f = open(URILLIST_FILE,'r')
count=0
for uri in f:
    
    req = urllib2.Request( URL_PREFIX + uri)
    try:
        resp = urllib2.urlopen(req)
    except urllib2.URLError, e:
        if e.code == 404 :
            count +=1
            print "ERROR_CODE: " + str(e.code) + " URL: " + URL_PREFIX + uri
            print count
        else:
            print "200 URL: " + URL_PREFIX + uri
        body = resp.read()

if count != 0:
   exitFunction ("404 found. total:"+str(count))

f.close()
return count

        
# def main():
#  parser = argparse.ArgumentParser()
#  parser.add_argument("--file", type=str, help="The file to upload e.g. /tmp/file.log")
#  args = parser.parse_args()
#  
#  if args.verbose :
#   print ("file: [%s]" % args.file)
# 
# if (not args.file ) :
#     exitFunction ("The File is mandatory")
#     if args.file :
#         Urllist = 