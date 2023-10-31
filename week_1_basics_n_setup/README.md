# Docker

### [DE Zoomcamp 1.2.1 - Introduction to Docker](https://www.youtube.com/watch?v=EYNwNlOrpr0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=4)

-it (interactive mode, i: interactive, t: terminal) => able to type something \

> docker run -it ubuntu bash \
> docker run -it python:3.9

have a bash prompt and can execute commands, e.g. _pip install pandas_

> docker run -it --entrypoint=bash python:3.9

when exit the prompt and execute the command above again -> import pandas -> there's no module named pandas -> since we run the container at the state before it's installed pandas. This particular image doesn't have pandas \

**=> Need something to make sure that the pandas library is there when we run our program => Using Dockerfile**

#### Dockerfile

```
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
```

Build docker image

> docker build -t <image_name>:<tag> . (. means docker searches for dockerfile in current directory and builds an image) \
> EX: docker build -t test:pandas .

Run docker image with passing arguments

> docker run -it test:pandas 2023-10-31

# Google Cloud Platform

### [DE Zoomcamp 1.1.1 - Introduction to Google Cloud Platform](https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=3)

- Cloud computing services offered by google
- Includes a range of hosted services for compute, storage and application development that run on Google hardware
- Same hardware on which google runs its service
