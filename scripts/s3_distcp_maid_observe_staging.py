#!/usr/local/bin/python2.7
import os
import boto3
import shutil
from botocore.errorfactory import ClientError
import subprocess
import sys
import json
import requests
import gnupg
from datetime import date, timedelta, datetime

# S3 bucket name
LMS_BUCKET = "drawbridge-lms-16218"

# Temporary local storage for S3 MAID files
WORKDIR = "/home/oozie/db_li_maid_observe/files"
# HDFS destionation dir for S3 MAID files
#HDFSDIR = "/user/oozie/profile/li_maids_observation"
HDFSDIR = "/user/oozie/s3-hdfs/profile/li_maids_observation"

# Initialize oozie keytab
os.system("kinit -kt /home/oozie/oozie.keytab oozie/graph-edge1.sc2.drawbrid.ge@DRAWBRID.GE")


def get_s3_creds_from_vault():
  secrets = {}

  with open('/home/oozie/db_li_maid_observe/conf/config.json') as file_config:
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
  subprocess.check_call(put_cmd)


#def successfile(success_file):
#  success_cmd = 'hadoop fs -touchz'.split( ' ' )
#  success_cmd.append(success_file)
#  subprocess.check_call(success_cmd)
#def successfile():
#   success_cmd = 'hadoop fs -touchz'.split( ' ' )
#   successfile = HDFSDIR + '/' + todaydate + '/_SUCCESS'
#   success_cmd.append(successfile)
#   p = subprocess.Popen(success_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#   p.communicate()


def cleanup(workdir):
  print "Deleting {0}".format(os.path.dirname(workdir))
  shutil.rmtree(os.path.dirname(workdir))


def main():
  # Setup Secrets
  s3_creds = get_s3_creds_from_vault()
  s3id = s3_creds[0]
  s3secret = s3_creds[1]
  pphrase = s3_creds[2]

  s3 = boto3.client('s3', aws_access_key_id=s3id, aws_secret_access_key=s3secret)
  #Override date to pull here.
  #todaydate = '2020-02-01'
  # The date stamp will be 2 days behind from when ingestion begins
  todaydate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
  now_date = (datetime.now().strftime('%Y-%m-%d'))
  for maid in ["daily"]:
    workdir = "{0}/{1}/".format(WORKDIR, todaydate)
    dir_prefix = 'maids-observation/ImpressionLog/{0}/{1}/'.format(maid, todaydate)
    metafile_prefix = dir_prefix + 'metadata.json'
    file_list = []

    try:
      a = s3.head_object(Bucket=LMS_BUCKET, Key=metafile_prefix)
    except ClientError:
      print "CRIT: S3 file {0} is missing.".format(metafile_prefix)
      sys.exit(1)

    os.mkdir(os.path.dirname(workdir))
    #os.mkdir(workdir)

    metadata_file = "{0}/metadata.json".format(workdir)
    try:
      s3.download_file(LMS_BUCKET, metafile_prefix, metadata_file)
      with open(metadata_file) as mfile:
        metadata = json.load(mfile)

        for key, value in metadata['dataset'].iteritems():
          if key == 'Num-Files':
            expected_num_files = value

        for key in metadata['file'].keys():
          file_list.append(key)
          listed_num_files = len(file_list)

        if expected_num_files == listed_num_files:
          print "Num-List: %s Dataset-List: %s" % ( expected_num_files, listed_num_files )

    except ClientError:
      print "CRTI: Unable to download/parse {0} file.".format(metadata_file)
      sys.exit(1)

    try:
      for f in file_list:
        gpg_file = dir_prefix + f
        print "GPG File: {0}".format(gpg_file)
        file_loc = workdir + '/' + f
        print "Local file: {0}".format(file_loc)
        s3.download_file(LMS_BUCKET, gpg_file, file_loc)
        gzfile = decrypt_file(pphrase, file_loc)
        os.remove(file_loc)
        filetoput = rename_file(gzfile)
        print "Decrypted file: {0}".format(filetoput)
      success_file = workdir + '/_SUCCESS'
      open(success_file, 'a').close()

    except:
      print "Error Processing Files"
      pass

    os.remove(metadata_file)
    hdfsmkdir("{0}/{1}/".format(HDFSDIR, now_date))
    print "workdir is {0}".format(workdir)
    hdfsput(workdir, "{0}/{1}/".format(HDFSDIR, now_date))
    cleanup(workdir)


if __name__ == "__main__":
  main()
