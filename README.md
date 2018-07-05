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

## Running Tests
Run:

`make test`

## Getting Data from Gen3 and Loading it

1. The first step is to extract the Gen3 data you want using the
   [sheepdog exporter](https://github.com/david4096/sheepdog-exporter). The TopMed public data extracted
   from sheepdog is available [on the release page](https://github.com/david4096/sheepdog-exporter/releases/tag/0.3.1)
   under Assets. Assuming you use this data, you will now have a file called `topmed-public.json`

1. Make sure you are running the virtual environment you set up in the **Setup** instructions.

1. Now we need to transform the data. We can transform to the outdated gen3 format, or to the new standard format.

    - For the standard format, follow instructions at
      [newt-transformer](https://github.com/jessebrennan/newt-transformer#transforming-data-from-sheepdog-exporter).

    - For the old Gen3 format, run this from the root of the project:

      ```
      python transformer/gen3_transformer.py /path/to/topmed_public.json --output-json transformed-topmed-public.json
      ```

1. Now that we have our new transformed output we can run it with the loader.

    If you used the standard transformer use the command:

   ```
   python scripts/cgp_data_loader.py --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET standard --json-input-file transformed-topmed-public.json
   ```

   Otherwise for the outdated gen3 format run:

   ```
   python scripts/cgp_data_loader.py --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET gen3 --json-input-file transformed-topmed-public.json
   ```
   
1. You did it!
