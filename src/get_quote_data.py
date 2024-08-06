import configparser as cfg

from utils.config_parser import default_parser

###########################################################
# parse command line arguments
###########################################################

parser = default_parser(description="Save some marketdata to disk.")

args = vars(parser.parse_args())

output_suffix = args["output_suffix"]
config_file = args["config_file"]

###########################################################
# grab initial values from config file
###########################################################

config = cfg.ConfigParser()
config.read(config_file)

api_key = config.get("alpaca", "api_key")
secret_key = config.get("alpaca", "secret_key")

###########################################################
# get quote data
###########################################################

print(f"api_key: {api_key}, secret_key: {secret_key}")
