# based image python ver 3.9
FROM python:3.9

# Run command to install pandas inside the container
# and it will create new image based on that
RUN pip install pandas

# Specify the working directory, which is the location in the container
# where the file will be copied to
WORKDIR /app

# Copy this file from current working directory to the docker image
# 1st argument: the name in the source on host machine
# 2nd argument: the name on the destination
COPY pipeline.py pipeline.py

# Overwrite entrypoint to get bash prompt (or execute python file)
# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "pipeline.py"]