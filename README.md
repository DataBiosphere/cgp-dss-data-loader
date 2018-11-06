# cgp-dss-data-loader
Simple data loader for CGP HCA Data Store

## Common Setup
1. **(optional)**  We recommend using a Python 3
   [virtual environment](https://docs.python.org/3/tutorial/venv.html).

1. Run:

   `pip3 install cgp-dss-data-loader`

## Setup for Development
1. Clone the repo:

   `git clone https://github.com/DataBiosphere/cgp-dss-data-loader.git`

1. Go to the root directory of the cloned project:

   `cd cgp-dss-data-loader`

1. Make sure you are on the branch `develop`.

1. Run (ideally in a new [virtual environment](https://docs.python.org/3/tutorial/venv.html)):

   `make develop`

## Cloud Credentials Setup
Because this program uses Amazon Web Services and Google Cloud Platform, you will need to set up credentials
for both of these before you can run the program.

### AWS Credentials
1. If you haven't already you will need to make an IAM user and create a new access key. Instructions are
   [here](https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html).

1. Next you will need to store your credentials so that Boto can access them. Instructions are
   [here](https://boto3.readthedocs.io/en/latest/guide/configuration.html).

### GCP Credentials
1. Follow the steps [here](https://cloud.google.com/docs/authentication/getting-started) to set up your Google
   Credentials.

## (Optional) Cloud Metadata Credentials Setup
When the loader submits data, it actually needs access to the referenced files in the account to obtain metadata 
(e.g. hash and size) that may be in another account.

If the data is public, this is unnecessary.  However, if access is controlled, additional credentials must be provided.

If using metadata credentials, it's strongly encouraged to perform a dry run first as a test.  This will ensure your 
credentials are correct.

### (Optional) GCP Metadata Credentials
If GCP files are being loaded that specifically require Google user credentials 
(rather than Google Service Account credentials), perform the following steps:

1. Make sure you have [gcloud](https://cloud.google.com/sdk/install) installed.

1. Run

   ```gcloud auth application-default login```

1. Follow the link to the account accessed.

1. This will generate a json with your user credentials with a path similar to:

    `/home/<user>/.config/gcloud/application_default_credentials.json`

1. Copy this json to another location so that it will not accidentally be used as a default by the main application.

1. This file can then be used by the loader by specifying (as an example):

    `--gcp-metadata-cred /home/<user>/metadata_credentials/my_user_credentials.json`

### (Optional) AWS Metadata Credentials
For when AWS files are being loaded that require assuming a role for access.

**One caveat, AWS allows a maximum of 12 hours under an assumed role for a single session, so 
if loading takes longer than that, it may break.

This involves the setup of an AssumedRole on the account that your main AWS credentials have access to.  If 
this is done already, all you need to do is supply a file containing the AWS ARN to that assumed role and the
loader will assume the role on your behalf when gathering information about the metadata.

Additional information on setting up an AssumedRole through AWS:
- https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html
- https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html

If AWS files are being loaded that require assuming a role for access, perform the following steps:

1. Write a file containing the ARN, for example:

    `arn:aws:iam::************:role/ROLE_NAME_HERE`

1. This file can then be used by the loader by specifying (as an example):

    `--aws-metadata-cred /home/<user>/aws_credentials.config`

## Running Tests
Run:

`make test`

## Getting Data from Gen3 and Loading it

1. The first step is to extract the Gen3 data you want using the
   [sheepdog exporter](https://github.com/david4096/sheepdog-exporter). The TopMed public data extracted
   from sheepdog is available [on the release page](https://github.com/david4096/sheepdog-exporter/releases/tag/0.3.1)
   under Assets. Assuming you use this data, you will now have a file called `topmed-public.json`

1. Make sure you are running the virtual environment you set up in the **Setup** instructions.

1. Now you will need to transform the data into the 'standard' loader format. Do this using the
   [newt-transformer](https://github.com/jessebrennan/newt-transformer).
   You can follow the [common setup](https://github.com/DataBiosphere/newt-transformer#common-setup), then the
   section for [transforming data from sheepdog](https://github.com/jessebrennan/newt-transformer#transforming-data-from-sheepdog-exporter).

1. Now that we have our new transformed output we can run it with the loader.

   If accessing public access data, use the command:

   ```
   dssload --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET transformed-topmed-public.json
   ```
   
   Alternatively, if supplying additional credentials for private data:

   ```
   dssload --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET -p GOOGLE_PROJECT_ID --gcp-metadata-cred gs_credentials.json --aws-metadata-cred aws_credentials.config gtex.json
   ```

1. You did it!
