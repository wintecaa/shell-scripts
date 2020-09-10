#!/usr/local/bin/python2.7
import argparse
import boto3
import gnupg
import json
import os
import requests
import shutil
import subprocess
import sys

from botocore.errorfactory import ClientError
from datetime import date, timedelta, datetime

# S3 bucket name
LMS_BUCKET = "drawbridge-lms-16218"

# Temporary local storage for S3 files
WORKDIR = "/home/oozie/db_li_membersessionevent/files"
# HDFS destionation dir for S3 files
HDFSDIR = "/user/oozie/s3-hdfs/profile/li_dbs_maid_observation"

# Initialize the appc08ebd2 service keytab
kinit_cmd_str = "kinit -kt /home/oozie/oozie.keytab oozie/graph-edge1.sc2.drawbrid.ge@DRAWBRID.GE"

if os.system(kinit_cmd_str) != 0:
  print "error running running kinit command '{}', exiting".format(kinit_cmd_str)
  sys.exit(1)


def get_s3_creds_from_vault():
  secrets = {}

  with open('/home/oozie/db_li_membersessionevent/conf/config.json') as file_config:
    data_config = json.load(file_config)

  # Authenticate to Vault
  url = data_config['vault.Url'] + "/v1/auth/approle/login"
  data = {'role_id': data_config['vault.RoleId'], 'secret_id': data_config['vault.SecretId']}
  r = requests.post(url=url, data=json.dumps(data))
  response_json = json.loads(r.text)

  # Get Vault secrets
  client_token = response_json['auth']['client_token']
  url = data_config['vault.Url'] + "/v1/kv/data/" + data_config['vault.Secret']
  headers = {'X-Vault-Token': client_token}
  r = requests.get(url=url, headers=headers )
  response_json = json.loads( r.text )
  for key, value in response_json['data']['data'].iteritems():
    secrets[key] = value
  access_id = secrets['access']
  secret_id = secrets['secret']
  pphrase = secrets['gpgpass']

  return [access_id, secret_id, pphrase];


def decrypt_file(pphrase, gfile):
  ef = os.path.splitext(gfile)
  end_filename = ef[0]
  gpg = gnupg.GPG(gnupghome='/home/oozie/.gnupg')
  with open(gfile, 'rb') as f:
    status = gpg.decrypt_file(f, passphrase=pphrase, output=end_filename)
  return end_filename


def rename_file(gzipf):
  if ".gzip" in gzipf:
    gzf = gzipf.replace(".gzip", ".gz")
    os.rename(gzipf, gzf)
  return gzf


def hdfsmkdir(destination_dir):
  mkdir_cmd = "hdfs dfs -mkdir -p".split( ' ' )
  mkdir_cmd.append(destination_dir)
  subprocess.check_call(mkdir_cmd)


def hdfsput(source_dir, destination_dir):
  put_cmd = "hdfs dfs -copyFromLocal".split( ' ' )
  put_cmd.append(source_dir)
  put_cmd.append(destination_dir)
  print "Copying {0} to HDFS: {1}...".format(source_dir,destination_dir)
  subprocess.check_call(put_cmd)


def successfile(success_file):
  success_cmd = 'hadoop fs -touchz'.split( ' ' )
  success_cmd.append(success_file)
  subprocess.check_call(success_cmd)


def cleanup(workdir):
  print "Deleting {0}".format(os.path.dirname(workdir))
  shutil.rmtree(os.path.dirname(workdir))


def main():
  # Setup Secrets
  s3_creds = get_s3_creds_from_vault()
  s3id = s3_creds[0]
  s3secret = s3_creds[1]
  pphrase = s3_creds[2]

  client_s3 = boto3.Session(aws_access_key_id=s3id,aws_secret_access_key=s3secret)
  s3 = client_s3.resource('s3')
  db_li_bucket = s3.Bucket(LMS_BUCKET)

  parser = argparse.ArgumentParser()
  parser.add_argument("--date", help="{YYYY-MM-DD} data date to process")
  parser.add_argument("--today", action="store_true", help="runs ingestion job for current date")
  args = parser.parse_args()

  #todaydate = date.today().isoformat()
  todaydate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
  now_date = (datetime.now().strftime('%Y-%m-%d'))
  if args.today:
    todaydate = date.today().isoformat()
  if args.date:
    todaydate = args.date
  workdir = "{0}/{1}".format(WORKDIR, todaydate)
  #s3dir_prefix = 'MemberSessionEvent/{0}/'.format(todaydate)
  s3dir_prefix = 'MemberSessionEvent/' + todaydate + '/'

  try:
     objcount = 0
     for obj in db_li_bucket.objects.filter(Prefix=s3dir_prefix):
       objcount += 1
     if objcount == 0:
       print "CRIT: S3 path/files {0} are missing.".format(s3dir_prefix)
       sys.exit(1)

     print("Number of files to decrypt/move: {0}".format(objcount))
  except ClientError:
    print "CRIT: Unable to list bucket for {0}".format(s3dir_prefix)
    sys.exit(1)

  #os.mkdir(os.path.dirname(workdir))
  os.mkdir(workdir)

  try:
    for obj in db_li_bucket.objects.filter(Prefix=s3dir_prefix):
      gpg_file = os.path.basename(obj.key)
      print "GPG File: {0}".format(gpg_file)
      file_loc = workdir + '/' + os.path.basename(obj.key)
      print " - Local file: {0}".format(file_loc)
      s3.Bucket(LMS_BUCKET).download_file( s3dir_prefix + gpg_file, file_loc)
      gzfile = decrypt_file(pphrase, file_loc)
      os.remove(file_loc)
      print " -- Decrypted file: {0}".format(gzfile)
      #break
    success_file = workdir + '/_SUCCESS'
    open(success_file, 'a').close()
  except Exception as e:
    print "CRIT: Error Processing Files: {0}".format(e)
    cleanup(workdir)
    sys.exit(1)
    pass

  # Done decrypting. Ready to move to HDFS and cleanup
  print("Decryption complete.")
  hdfsmkdir("{0}/{1}".format(HDFSDIR, now_date))
  #hdfsput(workdir, "{0}".format(HDFSDIR))
  hdfsput(workdir, "{0}/{1}/".format(HDFSDIR, now_date))
  #successfile("{0}/{1}/_SUCCESS".format(HDFSDIR, now_date))
  cleanup(workdir)


if __name__ == "__main__":
  main()
