CHT Pipeline
========================

CHT Pipeline is a tool used to define data models for transforming the raw data we get from Couch DB into models that can then be queried to build dashboards

# Dev installation instructions

1. Follow the instructions in [cht-sync](https://github.com/medic/cht-sync) to set up the required environment variables
1. Run `docker-compose up` in the cht-sync directory to run the pipeline. Please ensure the `DATAEMON_INITAL_PACKAGE` env variable is set to your preferred branch of the cht-pipeline repo. 
