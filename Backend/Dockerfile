# Use AWS Lambda Python base image with specific SHA
FROM public.ecr.aws/lambda/python:3.10@sha256:b3a2eca8924028cb9327177e1633149fda42fa051e23aff027ad81473eb90857

# Install system dependencies
RUN yum update -y && \
    yum install -y wget unzip && \
    yum clean all

# Install pip packages
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scrape_logic_lambda.py ${LAMBDA_TASK_ROOT}/
COPY app.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD ["app.lambda_handler"]
