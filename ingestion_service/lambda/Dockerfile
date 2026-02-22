FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
COPY config.py ${LAMBDA_TASK_ROOT}/
COPY parser.py ${LAMBDA_TASK_ROOT}/
COPY filter.py ${LAMBDA_TASK_ROOT}/
COPY writer.py ${LAMBDA_TASK_ROOT}/

CMD ["lambda_function.lambda_handler"]
