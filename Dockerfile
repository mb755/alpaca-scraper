FROM continuumio/miniconda3

RUN mkdir -p alpaca-scraper

COPY . /alpaca-scraper
WORKDIR /alpaca-scraper

RUN apt-get update && apt-get install -y doxygen graphviz git

RUN conda env create --name alpaca-scraper --file environment.yml

RUN echo "conda activate alpaca-scraper" >> ~/.bashrc
SHELL ["/bin/bash", "--login", "-c"]

RUN pre-commit install
