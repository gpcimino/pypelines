import logging
import logging.config
import argparse
import sys
import os
import time
from .config import config2dict

figlet_txt = """
    ____                   ___                
   / __ \__  ______  ___  / (_)___  ___  _____
  / /_/ / / / / __ \/ _ \/ / / __ \/ _ \/ ___/
 / ____/ /_/ / /_/ /  __/ / / / / /  __(__  ) 
/_/    \__, / .___/\___/_/_/_/ /_/\___/____/  
      /____/_/                                
"""

def split_name_value_pairs(nv_pair, sep=os.pathsep):
    nv_pair = nv_pair.split(sep)
    return dict(s.split('=') for s in nv_pair)      

def main(inputfile, pypelines_variables):
    t1 = time.time()
    exec(open(inputfile).read(), globals()) #todo: fix this
    t2 = time.time()
    print(inputfile + " took " + str(t2-t1) + " seconds.")


if __name__ == '__main__':
    sep = os.pathsep
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logconf", nargs="?", default='stdout', help="Logger config file or stdout (default), none to log to /dev/null")
    parser.add_argument("-e", "--envconfig", nargs="?", default=None, help="Name of environment variable containing parameter as name1=value1"  + sep + "name2=value2"  + sep + "...")
    parser.add_argument("-p", "--params", nargs="?", default=None, help="Parameters as name1=value1"  + sep + "name2=value2"  + sep + "... ")
    parser.add_argument("-c", "--configfile", nargs="?", default=None, help="Config file path having Python configparser syntax")
    parser.add_argument("-i", "--interpolate", nargs="?", default=False, help="Interpolate config file values, i.e. values may contain format strings which refer to other values using %(<varname>)s syntax")
    parser.add_argument("-n", "--interpolateenvvar", nargs="?", default=None, help="Interpolate config file with environment variables listed using same sysntax as -i")

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
    log.info(figlet_txt)
    log.info("is starting!")
    log.info(loginfostring)

    variables_cmdline = {}
    variables_env = {}
    variables_configfile = {}

    #priority in case of param with same name cmdline, configfile, env var
    #env var 
    if args.envconfig is not None:
        var = os.getenv(args.envconfig)
        if not var:
            log.fatal("Cannot find environmental variable " + args.envconfig + " containing init parameters")
            sys.exit(1)
        variables_env = split_name_value_pairs(var)
        log.debug("Parameters from env variable: " + str(variables_env))

    #config file
    if args.configfile is not None:
        if os.path.exists(args.configfile):
            #variables_configfile = config2dict(args.configfile, args.interpolate)
            if args.interpolateenvvar is not None:
                env_var_interp = split_name_value_pairs(args.interpolateenvvar) 
            variables_configfile = config2dict(args.configfile, args.interpolate, env_var_interp, flat=True)
            #log.debug("Parameters from config file line: " + str(variables_configfile))
        else:
            log.fatal("Cannot find config file on path: " + str(args.configfile))
            sys.exit(2)

    #cmd line
    if args.params is not None:
        try:
            if args.params.strip() != "":
                variables_cmdline = split_name_value_pairs(args.params)
                log.debug("Parameters from cmd line: " + str(variables_cmdline))
        except Exception as ex:
            log.fatal("Cannot parse command line parameters")
            sys.exit(3)


    #union param dicts
    #caution: py3.5 only
    #pypelines_pars = {**variables_env, **variables_configfile}
    #pypelines_pars = {**pypelines_pars, **variables_cmdline}
    pypelines_pars = merge_two_dicts(variables_env, variables_configfile)
    pypelines_pars = merge_two_dicts(pypelines_pars, variables_cmdline)

    #print(__vars)
    main(args.inputfile, pypelines_pars)