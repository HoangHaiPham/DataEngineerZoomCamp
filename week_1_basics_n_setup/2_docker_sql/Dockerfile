# based image python ver 3.9
FROM python:3.9

# Install wget to download the csv file
RUN apt-get install wget

# Run command to install pandas inside the container
# and it will create new image based on that
# psycopg2 is a postgres db adapter for python: sqlalchemy needs it
RUN pip install pandas sqlalchemy psycopg2

# Specify the working directory, which is the location in the container
# where the file will be copied to
WORKDIR /app

# Copy this file from current working directory to the docker image
# 1st argument: the name in the source on host machine
# 2nd argument: the name on the destination
COPY ingest_data.py ingest_data.py

# Overwrite entrypoint to get bash prompt (or execute python file)
# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "ingest_data.py"]