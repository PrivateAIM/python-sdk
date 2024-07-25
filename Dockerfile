FROM python:3.11-buster

RUN pip install poetry==1.7.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN touch README.md

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

#COPY flame ./flame
#COPY resources ./resources
# clone the sdk from github
RUN git clone https://github.com/PrivateAIM/python-sdk.git
# move flame to the working directory
RUN mv python-sdk/flame ./
# move resources to the working directory
RUN mv python-sdk/resources ./



COPY tests/test_images/test_core_sdk_main.py ./

EXPOSE 8080

ENTRYPOINT ["poetry", "run", "python", "-m", "test_image_main"]
#ENTRYPOINT ["poetry", "run", "python", "-u", "test_core_sdk_main.py"]