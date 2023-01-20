### data-etl

It will contain separate folders with ETL dev for `CRBA` and `tmee` projects.

Packaging/Shipping: 
Artical which discribes how Packaging will work. 
https://jerrynsh.com/how-to-package-python-selenium-applications-with-pyinstaller/

Greate Expectation: 
The Greate Expectation Framework is used to validate the downloaded data. 
To use the interactive notebooks the following call logic is needed 
`` great_expectations -c crba_project/resources/great_expectations ...``



TODO: 
- Think about the config sigelton pattern https://charlesreid1.github.io/a-singleton-configuration-class-in-python.htm
- Build evaluation File for Indicator Completness
- Make data_in accesible via pkg_ressources

Errors: 
To be found under data_in/known_issus.csv