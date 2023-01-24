import atexit

import argparse

from crba_project.conf import Config
import crba_project.etl
from crba_project.log import configure_exception_log_handler, configure_exception_log_handler_short, configure_log_flow_full, configure_log_flow_stdout

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--use-remote-config",
        #TODO Make target configurable
        help="Use Remote config. Fixed Google Sheets URL",
        action="store_true",
        dest="remote_source_config"
    )
    parser.add_argument(
        "-o-dir", "--OutputDir", help="output_directory", default="data_out", dest="output_dir"
    )
    parser.add_argument(
        "-i-dir", "--InputDir", help="Directory with input files", default="data_in", dest="input_dir"
    )
    parser.add_argument(
        "-f",
        "--Filter",
        help="Expect a CSV File where the first col is the Source ID. The error.csv can be used",
        required=False,
        dest="filter"
    )
    parser.add_argument(
        "-l",
        "--LogLevel",
        help="The Log Level to be used on stdout",
        type=int,
        default=25,
        choices=range(0, 50),
    )
    parser.add_argument(
        "-c",
        "--caching",
        help="If the request should be cached",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-dry-run",
        help="do not run the etls",
        default=False,
        action="store_true",
    )

    return parser.parse_args()


if __name__ == "__main__":
    # Initialize parser

    args = parse_args()

    #Initialize Confige
    config = Config(**vars(args))

    def exit_handler():
        print(config.run_id)
    atexit.register(exit_handler)

    #Configure global Logging
    configure_log_flow_stdout(args.LogLevel)
    configure_exception_log_handler(config)
    configure_exception_log_handler_short(config)
    configure_log_flow_full(config)

    #exit(0)
    print(f"Number Sources:{config.source_config.shape}")

    #
    # BEGIN OF ETL
    #
    if not args.dry_run:
        crba_project.etl.run(config)
    


