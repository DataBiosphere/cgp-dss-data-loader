# cgp-dss-data-loader
Simple data loader for CGP HCA Data Store

## Common Setup
1. Clone the repo:

   `git clone https://github.com/DataBiosphere/cgp-dss-data-loader.git`

2. Go to the root directory of the cloned project:
   
   `cd cgp-dss-data-loader`

3. Run (ideally in a new [virtual environment](https://docs.python.org/3/tutorial/venv.html)):

   `pip install -r requirements.txt`

## Setup for Development
1. Clone the repo:

   `git clone https://github.com/DataBiosphere/cgp-dss-data-loader.git`
  
2. Go to the root directory of the cloned project:
   
   `cd cgp-dss-data-loader`
   
3. Make sure you are on the branch `develop`.
  
4. Run (ideally in a new [virtual environment](https://docs.python.org/3/tutorial/venv.html)):

   `pip install -r requirements-dev.txt`

## Running Tests
Run:

`make test`

## Getting data from Gen3 and Loading it

1. The first step is to extract the Gen3 data you want using the 
   [sheepdog exporter](https://github.com/david4096/sheepdog-exporter). The TopMed public data extracted
   from sheepdog is available [on the release page](https://github.com/david4096/sheepdog-exporter/releases/tag/0.3.1)
   under Assets. Assuming you use this data, you will now have a file called `topmed-public.json`
   
2. Make sure you are running the virtual environment you set up in the **Setup** instructions.

3. Now we need to transform the data. From the root of the project run:
   
   `python transformer/gen3_transformer.py /path/to/topmed_public.json --output-json transformed-topmed-public.json` 
   
4. Now that we have our new transformed output we can run it with the loader.

   `python scripts/cgp_data_loader.py --no-dry-run --dss-endpoint MY_DSS_ENDPOINT --staging-bucket NAME_OF_MY_S3_BUCKET gen3 --json-input-file transformed-topmed-public.json`
   
5. You did it!
