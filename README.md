# Data-etl

It will contain separate folders with ETL dev for `CRBA` and `tmee` projects.

## Packaging/Shipping: 
Artical which discribes how Packaging will work. 
https://jerrynsh.com/how-to-package-python-selenium-applications-with-pyinstaller/

## Indicator Dictonary: 
The Indicator Dictonary is the central point to configure the inputs of this ETL. 
One version in inside the data_in folder. But the main version can be found as a Google sheet. 
### Parametrisation
The endpoint of the sources on the source sheet can be parameterized. Therfore the Enpoint can be just modified by a simple `{var_name}`
In the indicator sheet there need to be a column with the name `param_<var_name>`. In the coressponding row for the source a value can be defined. 


## Validation with Greate Expectation: 
The Greate Expectation Framework is used to validate the downloaded data. 
To use the interactive notebooks the following call logic is needed 
`` great_expectations -c crba_project/resources/great_expectations ...``

TODO: 
- Think about the config sigelton pattern https://charlesreid1.github.io/a-singleton-configuration-class-in-python.htm
- Build evaluation File for Indicator Completness
- Make data_in accesible via pkg_ressources

Errors: 
To be found under data_in/known_issus.csv