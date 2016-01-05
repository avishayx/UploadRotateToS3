#!/usr/bin/python
from os.path import basename
import argparse
import tinys3
import os

def getPartialPath(filename,numOfParents):
    numOfParents=numOfParents+1
    partialPath='/'+'/'.join([str(x) for x in filename.split("/")[numOfParents:]])
    return partialPath

def uploadFileToS3(fileToUpload,awsKey, awsPass, awsBucket,awsEndpoint,fileDestination):

    fileToUpload = fileToUpload.strip()
    fileBaseName = basename(fileToUpload).strip()
    if fileDestination == "/":
        fileLogicalPath =  fileBaseName
    elif fileDestination.isdigit():
        fileLogicalPath = getPartialPath(fileToUpload,int(fileDestination))# remove 2 parent dirs, e.g. remove /var/logs from /var/logs/archaive_dc_logs/0/20141218/ActivateUserRequest/data-collect-REST__ActivateUserRequest.2014-12-18-10-31.log.gz
    else:
        fileLogicalPath =  fileDestination + "/" + fileBaseName
    
    f = open(fileToUpload,'rb')
    conn = tinys3.Connection(awsKey,awsPass,tls=True,endpoint=awsEndpoint)
    try:
        result = conn.upload(fileLogicalPath, f,awsBucket)
        print ("The upload result is %s for [%s]" % (result,fileLogicalPath))
    except :
        print ("ERROR: [%s] was not uploaded" % fileToUpload)        
    return

def uploadListToS3(listToUpload, awsKey, awsPass, awsBucket, awsEndpoint, fileDestination):
    with open(listToUpload) as listOfFiles:
        for line in listOfFiles:
            try:
                if (not line.startswith('#')) and (not line.startswith('\n')) :
                    uploadFileToS3(line, awsKey, awsPass, awsBucket, awsEndpoint, fileDestination)
            except :
                print ("ERROR: [%s] was not uploaded" % line.strip())
    return
		
def exitFunction(msg):
  print (msg) 
  exit (1)
  return
 # Main

def main():
 parser = argparse.ArgumentParser()
 parser.add_argument("--file", type=str, help="The file to upload e.g. /tmp/file.log")
 parser.add_argument("--list", type=str, help="The list is a file which contains list of files")
 parser.add_argument("--destination",type=str, default='/', help="The Destination of the file to upload")
 parser.add_argument("--awsEndpoint", type=str, default='s3-us-west-2.amazonaws.com', help="The aws endpoint")
 parser.add_argument("--bucket", type=str, help="The S3 Bucket Name")
 parser.add_argument("--awsKey", type=str, help="AWS_ACCESS_KEY_ID, The aws key")
 parser.add_argument("--awsPass", type=str, help="AWS_SECRET_ACCESS_KEY, The aws pass")
 parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
 
 args = parser.parse_args()

 if args.verbose :
  print ("file: [%s]" % args.file)
  print ("list: [%s]" % args.list)
  print ("destination: [%s]" % args.destination)
  print ("awsEndpoint: [%s]" % args.awsEndpoint)
  print ("bucket: [%s]" % args.bucket)
  print ("awsKey: [%s]" % args.awsKey)
  print ("awsPass: [%s]" % args.awsPass)
 
  if (not args.file ) and ( not args.list ) :
    exitFunction ("The File or List is mandatroy")
  
  if not args.bucket :
    exitFunction ("The aws bucket is mandatory")
  
  if not args.awsKey :
    exitFunction ("The aws key is mandatory")
  
  if not args.awsPass :
    exitFunction ("The aws pass is mandatory")
  
#Upload args.file from the user to S3
 if args.file : 
   uploadFileToS3(args.file,args.awsKey, args.awsPass, args.bucket,args.awsEndpoint,args.destination)

#Upload all files in args.list to S3
 if args.list :
     uploadListToS3(args.list,args.awsKey, args.awsPass, args.bucket,args.awsEndpoint,args.destination)
 return

if __name__ == "__main__":
    main()



