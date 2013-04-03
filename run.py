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

_, path, file, credentialFile, logFile, gearmanFile, awsFile = argv

""" user config """
credentialConfig = ConfigParser.RawConfigParser()
credentialConfig.read(credentialFile)

""" gearman config """
gearmanConfig = ConfigParser.RawConfigParser()
gearmanConfig.read(gearmanFile)

""" aws config """
awsConfig = ConfigParser.RawConfigParser()
awsConfig.read(awsFile)

""" open log file """
f = open(logFile, 'a+')
f.write('>>> %s %s starting import\n' % (path, file))

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
  k.set_contents_from_filename('%s/%s' % (path, file))
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
  os.rename('%s/%s' % (path, file), '%s/%s' % (processedDir, file))
  f.write('    %s %s queueing up\n' % (path, file))
else:
  f.write('    %s %s could not get credentials\n' % (path, file))

""" close the log file """
f.write('<<< %s %s finished\n' % (path, file))
f.close()
