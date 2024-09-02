# My Streamlit Project

This project is a Streamlit app that loads parquet table data from AWS S3 and provides a dashboard for viewing data. The project also includes an upload script for uploading data to S3, an tranfer_data script for transferring table data from PostgreSQL to tables in the formats including parquet, csv and json.

## File Structure

- **.streamlit/**: Configuration files for Streamlit.
  - `config.toml`: Configuration for running the Streamlit app.
  - `secrets.toml`: Secrets file for local development (not committed).
- **streamlit_app/**: Contains the main Streamlit app.
  - `main.py`: Main Streamlit app script.
  - `s3_loader.py`: Python script to load the most recent parquet files from an S3 bucket to pandas data frame
- **upload_script/**: Contains the script for uploading data to S3.
  - `upload_to_s3.py`: Python script to upload files to S3.
- **transfer_data/**: Contains the script for transferring table data from PostgreSQL to tables in parquet, csv, json format.
  - `transfer_data.py`: Python script to transfer data into parquet, csv and json format.
- **terraform/**: Contains the tf files for deploy AWS S3 resource.
  - `main.tf`: infrastructure of aws provider.
  - `s3.tf`: infrastructure of aws s3
- **.github/**: Contains GitHub Actions workflows for CI/CD.
  - `deploy.yml`: Workflow to automate data upload and app deployment.
- **Makefile**: Automates tasks like setting up the environment, running the app, and deploying.
- **.gitignore**: Specifies files and directories to ignore in version control.
- **README.md**: Project documentation and instructions.

## Live Demo

You can access the Real time version of the data app using the following link:

[Real time Data App](https://de-alapin-totesys-team-data-app.streamlit.app/)


## Further development Later----Automating with Makefile
The Makefile included in this project automates common tasks:


## Further development Later----Continuous Deployment with GitHub Actions
This project uses GitHub Actions for continuous deployment. 
The workflow (.github/workflows/deploy.yml) is triggered when changes are pushed to the main branch. 
The workflow will:

1. Check out the code.
2. Install dependencies.
3. Upload data files to S3.
4. Load data from parquet files in S3 to dataframe.
5. Deploy the Streamlit app.

