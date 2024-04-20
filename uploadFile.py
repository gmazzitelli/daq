#!/usr/bin/env python3
#
# G. Mazzitelli 2024
# WC DAQ
#


def get_s3_sts(client_id, client_secret, endpoint_url, session_token):
    # Specify the session token, access key, and secret key received from the STS
    import boto3
    sts_client = boto3.client('sts',
            endpoint_url = endpoint_url,
            region_name  = ''
            )

    response_sts = sts_client.assume_role_with_web_identity(
            RoleArn          = "arn:aws:iam:::role/S3AccessIAM200",
            RoleSessionName  = 'cygno',
            DurationSeconds  = 3600,
            WebIdentityToken = session_token # qua ci va il token IAM
            )

    s3 = boto3.client('s3',
            aws_access_key_id     = response_sts['Credentials']['AccessKeyId'],
            aws_secret_access_key = response_sts['Credentials']['SecretAccessKey'],
            aws_session_token     = response_sts['Credentials']['SessionToken'],
            endpoint_url          = endpoint_url,
            region_name           = '')
    return s3

def upload_file_2_S3(file_name, client_id, client_secret,  endpoint_url, bucket, tag, tfile, verbose=False):

    with open(tfile) as file:
        token = file.readline().strip('\n')
    session_token= token
    if (verbose): print("TOKEN > ",tfile, token)
    s3 = get_s3_sts(client_id, client_secret, endpoint_url, session_token)
    filename = file_name.split('/')[-1]
    try:
        s3.upload_file(file_name, Bucket=bucket, Key=tag+'/'+filename)
        return 0
    except Exception as e:
        print('ERROR S3 file update: {:s} --> '.format(file_name), e)
        return 1

    
def main(filename, bucket, tag, remove, gzip, verbose):
    import os
    import gzip
    import shutil
    f2remove = filename
    if gzip:
        with open(filename, 'rb') as f_in:
            with gzip.open(filename+'.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        filename = filename+'.gz'
            
    upload_file_2_S3(filename, os.environ['IAM_CLIENT_ID'], os.environ['IAM_CLIENT_SECRET'],  
                 'https://minio.cloud.infn.it/', bucket, tag, '/tmp/token', verbose=verbose)
    if remove:
        os.remove(f2remove)
    
if __name__ == "__main__":
    from optparse import OptionParser
    #
    # deault parser value
    #
    TAG         = 'WC'
    BUCKET      = 'cygno-analysis'


    parser = OptionParser(usage='usage: %prog [-b [{:s}] -t [{:s}] -v]\n'.format(BUCKET,TAG))
    parser.add_option('-b','--bucket', dest='bucket', type='string', default=BUCKET, help='PATH to raw data')
    parser.add_option('-t','--tag', dest='tag', type='string', default=TAG, help='tag where dir for data')
    parser.add_option('-r','--remove', dest='remove', action="store_true", default=False, help='remove origin')
    parser.add_option('-g','--gzip', dest='gzip', action="store_true", default=False, help='gzip before upload')
    parser.add_option('-v','--verbose', dest='verbose', action="store_true", default=False, help='verbose output')
    (options, args) = parser.parse_args()
    if options.verbose: 
        print ("options", options)
        print ("args", args)
    if len(args)<1:
        parser.error("missing filename")
        sys.exit(1)
        
    main(args[0], options.bucket, options.tag, options.remove, options.gzip, options.verbose)
