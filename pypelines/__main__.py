import logging
import logging.config
import argparse
import sys
import os
import time

# figlet_txt = """
# ______                 _ _                 
# | ___ \               | (_)                
# | |_/ /   _ _ __   ___| |_ _ __   ___  ___ 
# |  __/ | | | '_ \ / _ \ | | '_ \ / _ \/ __|
# | |  | |_| | |_) |  __/ | | | | |  __/\__ \\
# \_|   \__, | .__/ \___|_|_|_| |_|\___||___/
#        __/ | |                             
#       |___/|_|                             
# """
figlet_txt = """
    ____                   ___                
   / __ \__  ______  ___  / (_)___  ___  _____
  / /_/ / / / / __ \/ _ \/ / / __ \/ _ \/ ___/
 / ____/ /_/ / /_/ /  __/ / / / / /  __(__  ) 
/_/    \__, / .___/\___/_/_/_/ /_/\___/____/  
      /____/_/                                
"""


def main(inputfile, pypelines_variables):
    t1 = time.time()
    # print("----------------------")
    # print(globals())
    # print("----------------------")
    # print(variables)
    # print("----------------------")
    # global_and_variables = {**variables, **globals()}
    # print(global_and_variables)
    # print("----------------------")
    # print(global_and_variables['source_path'])
    # print("----------------------")
    
    exec(open(inputfile).read(), globals())
    t2 = time.time()
    print(inputfile + " took " + str(t2-t1) + " seconds.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logconf", nargs="?", default='stdout', help="Logger config file or stdout (default), none to log to /dev/null")
    parser.add_argument("-e", "--envpar", nargs="?", default=None, help="Name of environment variable containing parameter as name1=value1;name2=value2;... as separator (';') use default path separator (e.g. ':' on UNIX, ';' on Windows) ")
    parser.add_argument("-p", "--parameters", nargs="?", default=None, help="List of parameters as name1=value1;name2=value2;...")

    parser.add_argument("inputfile", help="pypelines script")

    args = parser.parse_args()

    if args.logconf.lower() == 'stdout':
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        loginfostring = "Log to stdout"
    elif args.logconf.lower() == 'none':
        loginfostring = "Logging is not configured"
    else:
        logging.config.fileConfig(args.logconf)
        loginfostring = "Configure logging from " + args.logconf

    log = logging.getLogger(__name__)
    print("Ready to start... ")
    print(figlet_txt)
    print(loginfostring)

    variables_cmdline = {}
    variables_env = {}
    if args.parameters is not None:
        var = args.parameters.split(os.pathsep)
        variables_cmdline = dict(s.split('=') for s in var)
    log.debug("Parameters from cmd line: " + str(variables_cmdline))

    if args.envpar is not None:
        var = os.getenv(args.envpar)
        if not var:
            log.fatal("Cannot find environmental variable " + args.envpar + " containing init parameters")
            sys.exit(-1)
        var = var.split(os.pathsep)
        variables_env = dict(s.split('=') for s in var)
    log.debug("Parameters from env variable: " + str(variables_env))

    #union param dicts
    #variables = dict(variables_cmdline, **variables_env)
    #caution: py3.5 only
    __vars = {**variables_cmdline, **variables_env} #env overwrite cmdline
    main(args.inputfile, __vars)