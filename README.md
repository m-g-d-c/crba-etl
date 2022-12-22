### data-etl

This is the root repo.

It contains the `requirements.txt` file with libraries that we use.

It will contain separate folders with ETL dev for `CRBA` and `tmee` projects.

Packaging/Shipping: 
Artical which discribes how Packaging will work. 
https://jerrynsh.com/how-to-package-python-selenium-applications-with-pyinstaller/



TODO: 
- Think about the config sigelton pattern https://charlesreid1.github.io/a-singleton-configuration-class-in-python.html

- Understand why abstract Extractor class needs to be importat like this "from crba_project.extractor import Extractor" in order for Extraction Error Exception to be properly catched. Dont Understand why. https://chrisyeh96.github.io/2017/08/08/definitive-guide-python-imports.html
How does data_staged_raw works? Sources S-168, S-169 and S-170 
Work throught data_out/<run-id>/error.csv

Add the aggregation etl from main_refactoring.ipynb

--> Build evaluation File for Indicator Completness

- Make data_in accesible via pkg_ressources
- Load Source just once even if there are used for multiple Indicators??


Errors: 

To be found under data_in Known Issus