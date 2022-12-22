import atexit

import argparse

import crba_project.etl
from crba_project.etl import build_and_bootstrap_config,build_combined_normalized_csv
from crba_project.log import configure_exception_log_handler, configure_exception_log_handler_short, configure_log_flow_stdout

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o-dir", "--OutputDir", help="output_directory", default="data_out"
    )
    parser.add_argument(
        "-i-dir", "--InputDir", help="Directory with input files", default="data_in"
    )
    parser.add_argument(
        "-f",
        "--Filter",
        help="Expect a CSV File where the first col is the Source ID. The error.csv can be used",
        required=False,
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

    return parser.parse_args()


if __name__ == "__main__":
    # Initialize parser

    args = parse_args()

    #Initialize Confige
    config = build_and_bootstrap_config(input_dir=args.InputDir,output_dir=args.OutputDir, caching=args.caching, filter=args.Filter)

    def exit_handler():
        print(config.run_id)
    atexit.register(exit_handler)

    #Configure global Logging
    configure_log_flow_stdout(args.LogLevel)
    configure_exception_log_handler(config)
    configure_exception_log_handler_short(config)

    print(f"Number Sources:{config.source_config.shape}")

    #
    # BEGIN OF ETL
    #
    crba_project.etl.run(config)
    


