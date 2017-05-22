import logging
import logging.config
import argparse
import sys
import os
import time
from configparser import SafeConfigParser

figlet_txt = """
    ____                   ___                
   / __ \__  ______  ___  / (_)___  ___  _____
  / /_/ / / / / __ \/ _ \/ / / __ \/ _ \/ ___/
 / ____/ /_/ / /_/ /  __/ / / / / /  __(__  ) 
/_/    \__, / .___/\___/_/_/_/ /_/\___/____/  
      /____/_/                                
"""
def config2dict(config_file):
    log.info("Load configs from file:" + str(config_file))
    confparser = SafeConfigParser(defaults=os.environ) #this is dangerous ==> all sys env var are set up in default section
    confparser.read(config_file)
    sections = confparser.sections()
    sections.append('DEFAULT')
    variables_cmdline = {s:dict(confparser.items(s)) for s in sections}
    variables_cmdline = {**variables_cmdline, **dict(confparser.items('DEFAULT'))} #default overwrite variables_cmdline
    #log.info("Params found in files: " + str(variables_cmdline))
    return variables_cmdline

def main(inputfile, pypelines_variables):
    t1 = time.time()
    exec(open(inputfile).read(), globals()) #todo: fix this
    t2 = time.time()
    print(inputfile + " took " + str(t2-t1) + " seconds.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logconf", nargs="?", default='stdout', help="Logger config file or stdout (default), none to log to /dev/null")
    parser.add_argument("-e", "--envconfig", nargs="?", default=None, help="Name of environment variable containing parameter as name1=value1;name2=value2;... as separator (';') use default path separator (e.g. ':' on UNIX, ';' on Windows) ")
    parser.add_argument("-p", "--params", nargs="?", default=None, help="Parameters as name1=value1;name2=value2;... ")
    parser.add_argument("-c", "--configfile", nargs="?", default=None, help="Config file filepath having Python configparser syntax")

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
    # if args.parameters is not None:
    #     if os.path.exists(args.parameters):
    #         variables_cmdline = config2dict(args.parameters)
    #     else:
    #         var = args.parameters.split(os.pathsep)
    #         variables_cmdline = dict(s.split('=') for s in var)
    # #log.debug("Parameters from cmd line: " + str(variables_cmdline))

    #priority in case of param with same name cmdline, configfile, env var
    #env var 
    if args.envconfig is not None:
        var = os.getenv(args.envconfig)
        if not var:
            log.fatal("Cannot find environmental variable " + args.envconfig + " containing init parameters")
            sys.exit(1)
        var = var.split(os.pathsep)
        variables_env = dict(s.split('=') for s in var)
        log.debug("Parameters from env variable: " + str(variables_env))

    #config file
    if args.configfile is not None:
        if os.path.exists(args.configfile):
            variables_configfile = config2dict(args.configfile)
            log.debug("Parameters from config file line: " + str(variables_configfile))
        else:
            log.fatal("Cannot find config file on path: " + str(args.configfile))
            sys.exit(2)

    #cmd line
    if args.params is not None:
        try:
            var = args.params.split(os.pathsep)
            variables_cmdline = dict(s.split('=') for s in var)
            log.debug("Parameters from cmd line: " + str(variables_cmdline))
        except Exception as ex:
            log.fatal("Cannot parse command line parameters")
            sys.exit(3)


    #union param dicts
    #variables = dict(variables_cmdline, **variables_env)
    #caution: py3.5 only
    #pypelines_pars = {**variables_cmdline, **variables_env} #env overwrite cmdline
    pypelines_pars = {**variables_env, **variables_configfile}
    pypelines_pars = {**pypelines_pars, **variables_cmdline}
    #print(__vars)
    main(args.inputfile, pypelines_pars)