import logging
import sys
import csv
import io 
import traceback

from crba_project.extractor import ExtractionError

root_package = __name__.split(".")[0]



def configure_exception_log_handler(config):
    #TODO Make the output more readable
    class CsvFormatter(logging.Formatter):
        def __init__(self):
            super().__init__()
            self.output = io.StringIO()
            self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL,quotechar="'",delimiter=";")

        def formatExceptionShort(self, ei):
            """
            Format and return the specified exception information as a string.

            This default implementation just uses
            traceback.print_exception()
            """
            sio = io.StringIO()
            tb = ei[2]
            # See issues #9427, #1553375. Commented out for now.
            #if getattr(self, 'fullstack', False):
            #    traceback.print_stack(tb.tb_frame.f_back, file=sio)
            traceback.print_exception(ei[0], ei[1], tb, 0, sio)
            s = sio.getvalue()
            sio.close()
            if s[-1:] == "\n":
                s = s[:-1]
            return s

        def format(self, record):
            self.writer.writerow([record.exc_info[1].source_id, self.formatExceptionShort(record.exc_info), self.formatException(record.exc_info)])
            data = self.output.getvalue()
            self.output.truncate(0)
            self.output.seek(0)
            return data.strip()

    extraction_error_log_handler = logging.FileHandler(f"{config.output_dir}/{config.run_id}/logs/extractor_error.log")
    
    extraction_error_log_handler.setFormatter(CsvFormatter())
    
    extraction_error_log_filter = logging.Filter(name="ExtractionError Filter")
    extraction_error_log_filter.filter = lambda record: record.exc_info and isinstance(record.exc_info[1],ExtractionError)
    extraction_error_log_handler.addFilter(extraction_error_log_filter)

    logging.getLogger().addHandler(extraction_error_log_handler)

def configure_exception_log_handler_short(config):
    #TODO Make the output more readable
    class CsvFormatter(logging.Formatter):
        def __init__(self):
            super().__init__()
            self.output = io.StringIO()
            self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL,quotechar="'",delimiter=";")

        def format(self, record):
            self.writer.writerow([record.exc_info[1].source_id, str(record.exc_info[1])])
            data = self.output.getvalue()
            self.output.truncate(0)
            self.output.seek(0)
            return data.strip()

    extraction_error_log_handler = logging.FileHandler(f"{config.output_dir}/{config.run_id}/logs/extractor_error_short.log")
    
    extraction_error_log_handler.setFormatter(CsvFormatter())
    
    extraction_error_log_filter = logging.Filter(name="ExtractionError Filter")
    extraction_error_log_filter.filter = lambda record: record.exc_info and isinstance(record.exc_info[1],ExtractionError)
    extraction_error_log_handler.addFilter(extraction_error_log_filter)

    logging.getLogger(root_package).addHandler(extraction_error_log_handler)

def configure_log_flow_stdout(log_level):
    class NoStackTraceFormatter(logging.Formatter):
        def format(self, record):
            return record.getMessage()

    logging.getLogger(root_package).setLevel(log_level)
    stout_logs = logging.StreamHandler(stream=sys.stdout)
    stout_logs.setFormatter(NoStackTraceFormatter())
    logging.getLogger(root_package).addHandler(stout_logs)
