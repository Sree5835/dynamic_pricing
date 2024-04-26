# dynamic_pricing

# Dynamic Pricing

## Introduction

This project is a mix of three parts: a webhook that satifies the requirements of the [Deliveroo Orders API](https://api-docs.deliveroo.com/docs/introduction), a simple Order Management System that interacts directly with the Orders DB, and Jupyter Notebooks for data analysis to measure the impact of Dynamic Pricing.

## Deliveroo

All the Deliveroo related decisions in this codebase are based on requirements from [Deliveroo Orders API](https://api-docs.deliveroo.com/docs/introduction).

## Requirements

- Python 3.10
- Key Dependencies: See `requirements.txt`

## Installation

You can install the Dynamic Pricing API by cloning the repository and installing the required dependencies.

```bash
git clone https://github.com/Sree5835/dynamic_pricing
cd dynamic_pricing
```

## Creating a virtual environment within the project

Using a virtual environment allows for a clean and isolated environment for you to install project dependencies and run code. To create a virtual environment within the project, run (depending on whether your Python command is `python` or `python3`):

```
python -m venv .venv
```

You can activate your virtual environment with:

```
source .venv/bin/activate
```

With Windows, use:

```
.venv\Scripts\activate
```

You can then install all dependencies and build within the virtual environment when activated. To deactivate, run:

```
deactivate
```

## Installing Dependencies

This project not only has exter dependencies in `requirements.txt`, it also has a dynamic_pricing package that is set up using the `setup.py`. Therefore, to install all depdencies and build, run:

```
pip install -r requirements.txt
pip install .
```

## Set up pre-commit

This repository uses the pre-commit library to enable pre-commit hooks with the project. These hooks allow for various checks to be completed when you run git commit, ensuring that no un-linted and un-formatted code is persisted to the remote repositories. We have two main hooks set up:

    Black (formatter)
    Pylint (linting)

You only need to set up pre-commit once when you clone this repo, by running:

```
pre-commit install
```

## Setting environment variables

This repository contains an `.env.example` file with example environment variables for usage in the code. The code will attempt to read from `.env` file at the top level of the repository. To ensure this file is present, run:

```
cp .env.example .env
```

## Running test

This project uses `pytest` as the unit testing framework. To run the unit tests, you can run:

```
pytest
```

We use pytest-cov as our coverage plugin. To run pytest and generate a HTML coverage report, you can run:

```
pytest --cov=src --cov-report=html
```
