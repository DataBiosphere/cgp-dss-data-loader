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

### AWS credentials
1. If you haven't already you will need to make an IAM user and create a new access key. Instructions are
   [here](https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html).

1. Next you will need to store your credentials so that Boto can access them. Instructions are
   [here](https://boto3.readthedocs.io/en/latest/guide/configuration.html).

### GCP credentials
1. Follow the steps [here](https://cloud.google.com/docs/authentication/getting-started) to set up your Google
   Credentials.

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

    If you used the standard transformer use the command:

   ```
   dssload --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET transformed-topmed-public.json
   ```

1. You did it!
