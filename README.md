# cgp-dss-data-loader
Simple data loader for CGP HCA Data Store

## common setup
1. **(optional)**  We recommend using a Python 3
   [virtual environment](https://docs.python.org/3/tutorial/venv.html).

1. Run:

   `pip3 install cgp-dss-data-loader`

## setup for development
1. clone the repo:

   `git clone https://github.com/databiosphere/cgp-dss-data-loader.git`

1. go to the root directory of the cloned project:

   `cd cgp-dss-data-loader`

1. make sure you are on the branch `develop`.

1. run (ideally in a new [virtual environment](https://docs.python.org/3/tutorial/venv.html)):

   `make develop`

## running tests
run:

`make test`

## getting data from gen3 and loading it

1. the first step is to extract the gen3 data you want using the
   [sheepdog exporter](https://github.com/david4096/sheepdog-exporter). the topmed public data extracted
   from sheepdog is available [on the release page](https://github.com/david4096/sheepdog-exporter/releases/tag/0.3.1)
   under assets. assuming you use this data, you will now have a file called `topmed-public.json`

1. make sure you are running the virtual environment you set up in the **setup** instructions.

1. now we need to transform the data. we can transform to the  outdated gen3 format, or to the new standard format.

    - for the standard format, follow instructions at
      [newt-transformer](https://github.com/jessebrennan/newt-transformer#transforming-data-from-sheepdog-exporter).

    - for the old gen3 format
      from the root of the project run:

      ```
      python transformer/gen3_transformer.py /path/to/topmed_public.json --output-json transformed-topmed-public.json
      ```

1. now that we have our new transformed output we can run it with the loader.

    if you used the standard transformer use the command:

   ```
   python scripts/cgp_data_loader.py --no-dry-run --dss-endpoint my_dss_endpoint --staging-bucket name_of_my_s3_bucket standard --json-input-file transformed-topmed-public.json
   ```

   otherwise for the outdated gen3 format run:

   ```
   python scripts/cgp_data_loader.py --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET gen3 --json-input-file transformed-topmed-public.json
   ```
   
1. You did it!
