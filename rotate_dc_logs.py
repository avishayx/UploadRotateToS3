#!/usr/bin/python
import os, sys
import shutil
import glob
import argparse
import socket
from uploadToS3 import *
from dmspZookeeperLib import *
from __builtin__ import file

def logRotate(sourcePath,destPath):
    print "logRotate function - started"
    if not os.path.exists(sourcePath):
        print ("The source folder does not exist [%s]" % sourcePath)
        return 1
    
    HostDesc=socket.gethostname() + "_" + socket.gethostbyname(socket.gethostname())

    for filename in os.listdir(sourcePath): #for path, subdirs, files in os.walk(r'/var/logs'):
        if filename.startswith("data-collect-REST__"):
            #Log Example: "data-collect-REST__HEALTHCECK_REQUEST.2015-01-20-07-24.log.gz"
            logPrefix="data-collect-REST__"
            logType="0"
        elif filename.startswith("data-collect-INT__"):
            logPrefix="data-collect-INT__"
            logType="1"
        elif filename.startswith("data-collect-RPC__"):
            logPrefix="data-collect-RPC__"
            logType="2"
        elif filename.startswith("component-dmsp-data-collection_external.service."):
            logPrefix="component-dmsp-data-collection_external.service."
            logType="3"
        else:
            continue
        
        basename = filename.split(".") # basename = ['data-collect-REST__HEALTHCECK_REQUEST', '2015-01-20-07-24', 'log', 'gz']
                                       # basename = ['component-dmsp-data-collection_external','service','log','2015-04-13-23-58','log','gz']

        #dirDate = basename[1].replace("-", "")[:8] #Output: "20150224" 
        if logType=="3":
            dirDate = basename[3][:10]
            ApiName = "external_service"
        else:
            dirDate = basename[1][:10] #Output: "2015-02-24"
            ApiName = basename[0].replace(logPrefix, "") #Output: "HEALTHCECK_REQUEST"
        newLogPath = destPath + "/" + logType + "/" + dirDate + "/" + ApiName + "/" + HostDesc
        # Create dist folder
        if not os.path.exists(newLogPath):
            os.makedirs ( newLogPath )
            print ("Folder [%s] was created" % newLogPath)
        
        src_file = os.path.join(sourcePath, filename)
        # Move File
        shutil.move(os.path.join(sourcePath + "/" + filename),newLogPath) #mv the file 
        print ("The file was moved from [%s] to [%s]" % (src_file,newLogPath))

    return 0

def moveLogsToArchive(src,dest):
#The Function get the source and destination for .logs file  in order to move 
    print "moveLogsToArchive function - started" 
    if not os.path.exists(src): #if the source does not exist
        print ("Source folder [%s] does not exists" % src)
        return 1

    if not os.path.exists(dest): #if the destination does not exist
        os.makedirs ( dest )
        print ("Folder [%s] was created" % dest)

    #Get the type of the log file.
    for type_dir in os.listdir(src): # for each type dir
#        if os.path.isdir(os.path.join(src, type_dir)): #If the the source and type dir exist
#            src_type_dir=os.path.join(os.path.join(src, type_dir)) #Get the path and files 
#            #Get the date of file.            
#            for date_dir in os.listdir(src_type_dir):
#                if os.path.isdir(os.path.join(src_type_dir, date_dir)):
#                    dest_dir=os.path.join(os.path.join(src_type_dir, date_dir))
#                    shutil.move(dest_dir,dest) 
#                    print ("The folder was moved from [%s] to [%s]" % (dest_dir,dest))

      # python-move-and-overwrite-files-and-folders
      root_src_dir = os.path.join(src, type_dir)
      root_dst_dir = dest 

      for src_dir, dirs, files in os.walk(root_src_dir):
         dst_dir = src_dir.replace(root_src_dir, root_dst_dir)
         if not os.path.exists(dst_dir):
             os.mkdir(dst_dir)
         for file_ in files:
             src_file = os.path.join(src_dir, file_)
             dst_file = os.path.join(dst_dir, file_)
             if os.path.exists(dst_file):
                 os.remove(dst_file)
             shutil.move(src_file, dst_dir)
             print ("The file was moved from [%s] to [%s]" % (src_file,dst_dir))

    return

def exitFunction(msg):
  print (msg) 
  exit (1)
  return
 
def createListOfArchivedFiles(pathToList):
    #Create A .list file that hold the files in "pathToList" for upload.   
    listFilesToUpload = pathToList + ".list"
    f = open(listFilesToUpload,'w')
    for dirpath, dirnames, files in os.walk(pathToList):
        for filename in files:
            results = os.path.join(dirpath, filename)
            # Write results to logfile
            f.write(results + "\n")
    return listFilesToUpload

def getAwsParamsFromZoo(): 
#Get parameters from Zoo-Keeper
    zooBase="/site/archive/logs/datacollection/aws"
    awsEndpoint=zkGet(zooBase+"/endpoint") # "s3-us-west-2.amazonaws.com" 
    awsBucket=zkGet(zooBase+"/bucket") # "lab-test-log-dc" 
    awsUser=zkGet(zooBase+"/user") #"AKIAJ6P3OIYAWLS2KEIQ"
    awsPass=zkGet(zooBase+"/password") #"eqF66qG8w73nrMA5rq4Uxi4LIlyqoUVzgZUDAKgU"
    
    if (not awsEndpoint) or (not awsBucket) or (not awsUser) or (not awsPass):
        exitFunction ("One of the parameters is missing") 
    else:
            return awsEndpoint, awsBucket, awsUser, awsPass

def uploadToS3(pathToUpload):  
 #Pars the parameters into Zoo-keeper 
  awsEndpoint, awsBucket, awsUser, awsPass = getAwsParamsFromZoo() #Parse the details to zoo
  listToUpload = createListOfArchivedFiles(pathToUpload)
  uploadListToS3(listToUpload,awsUser, awsPass, awsBucket,awsEndpoint,"3")
  
  return
  
  #Main

def main(): 
    #Key Function to present
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--source", type=str, default="/var/logs/archive", help="The logs source path in order to rotate")
    parser.add_argument("-r","--rotatePath", type=str, default="/var/logs/archive", help="The archive path")
    parser.add_argument("-ds3","--disableUploadToS3", action="store_true", default=False, help="False, to disable S3 function.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    args = parser.parse_args()

    logicalPath = args.source + "/archive_dc"
    rotatePath = args.rotatePath

            
    if args.verbose :
        print ("Source: [%s]" % args.source)
        print ("rotatePath: [%s]" % args.rotatePath)
        print ("uploadToS3: [%s]" % str(args.disableUploadToS3))

    #Source
    logRotate(args.source,logicalPath)
    #UploadToS3
    if not args.disableUploadToS3 :
        print "uploadToS3(logicalPath) has been started"
        uploadToS3(logicalPath)
    
    moveLogsToArchive(logicalPath,rotatePath) 

    return
        
           
if __name__ == "__main__":
    main()
