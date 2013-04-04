#!/usr/bin/python
from sys import argv
from openphoto import OpenPhoto 
from boto.s3.key import Key

import os
import sys
import binascii
import ConfigParser
import gearman
import json
import boto.s3
import boto.s3.key
import magic

_, path, file, credentialFile, logFile, gearmanFile, awsFile = argv
filePath = '%s/%s' % (path, file)

""" open log file """
f = open(logFile, 'a+')
f.write('>>> %s starting import\n' % (filePath))

""" check if file exists """
if not os.path.exists(filePath):
  f.write('    file does not exist %s\n' % filePath)
  sys.exit()

""" check mime type """
mime = magic.Magic(mime=True)
mimeType = mime.from_file(filePath)
if mimeType not in ['image/jpg','image/jpeg','image/tiff','image/png']:
  f.write('    invalid mime type for %s of %s\n' % (filePath, mimeType))
  sys.exit()

""" user config """
credentialConfig = ConfigParser.RawConfigParser()
credentialConfig.read(credentialFile)

""" gearman config """
gearmanConfig = ConfigParser.RawConfigParser()
gearmanConfig.read(gearmanFile)

""" aws config """
awsConfig = ConfigParser.RawConfigParser()
awsConfig.read(awsFile)

""" if the processed directory doesn't exist then we add it """
processedDir = '%s/processed' % path
if not os.path.exists(processedDir):
    os.makedirs(processedDir)

""" if the credential file has credentials we process """
if credentialConfig.has_section('credentials'):
  """ s3 client and upload """
  s3Conn = boto.connect_s3(awsConfig.get('s3','key'), awsConfig.get('s3','secret'))
  s3Bucket = s3Conn.get_bucket(awsConfig.get('s3', 'bucket'), validate=False)

  s3Path = '%s/%s.jpg' % (path, binascii.hexlify(os.urandom(16)))
  k = Key(s3Bucket)
  k.key = s3Path
  k.set_contents_from_filename(filePath)
  k.set_acl('public-read')

  """ gearman client """
  gearmanClient = gearman.GearmanClient(['%s:%s' % (gearmanConfig.get('gearman','host'), gearmanConfig.get('gearman','port'))])
  workload = {
      'op_host':credentialConfig.get('credentials','host'),
      'op_ckey':credentialConfig.get('credentials','consumerKey'),
      'op_csec':credentialConfig.get('credentials','consumerSecret'),
      'op_utok':credentialConfig.get('credentials','token'),
      'op_usec':credentialConfig.get('credentials','tokenSecret'),
      'email':credentialConfig.get('credentials','email'),
      'payload': {
          'photo':'http://%s.s3.amazonaws.com%s' % (awsConfig.get('s3','bucket'), s3Path)
        }
    }

  gearmanClient.submit_job("ImportToOpenPhotoWorker", json.dumps(workload), background=True)
  os.rename(filePath, '%s/%s' % (processedDir, file))
  f.write('    %s queueing up\n' % (filePath))
else:
  f.write('    %s could not get credentials\n' % (filePath))

""" close the log file """
f.write('<<< %s finished\n' % (filePath))
f.close()
