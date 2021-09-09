# One PI DaaS API Data SQL Generator

This is a SQL code generator tool used to automate the manual process of merging Excel and SQL scripts.

## Installation

Install Python full Windows 10 installer (https://docs.python.org/3/using/windows.html) --> Python3
Then enter the following commands on terminal:

```bash
cd <PATH>\daas-code-gen
python --version # ( this should be greater than >= 3 )
python -m venv env # ( if this command doesnt work try --> python3 -m venv env)
env\Scripts\activate
pip install -r requirements.txt
```

## GUI Access

To launch the application enter the following commands on terminal:

```bash
cd <PATH>\daas-code-gen
env\Scripts\activate
python apiApp.py
```

Then open any browser to: http://localhost:8080


## Jenkins Access

To launch the application enter the following commands on terminal:

```bash
cd <PATH>\daas-code-gen
python -m venv env # ( if this command doesnt work try --> python3 -m venv env)
env\Scripts\activate
pip install -r requirements_cli.txt
python apiAppCLI.py
```

## Create dynamically tables.yaml file for hasura-project by HASURA URL

To create the code enter the following commands on terminal:

```bash
cd <PATH>\daas-code-gen
python -m venv env # ( if this command doesnt work try --> python3 -m venv env)
env\Scripts\activate
pip install -r requirements_cli.txt
python apiApp.py
```
Then open any browser to: http://localhost:8080/generateYaml (this api will put the code into tables.yaml and will lunch metadata.sh(which is located on the hasura-project directory) by python code and apply metadata on hasura server using shell script.)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)