import logging
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


def main(inputfile):
    t1 = time.time()
    exec(open(inputfile).read(), globals())
    t2 = time.time()
    print(inputfile + " took " + str(t2-t1) + " seconds.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logconf", nargs="?", default='stdout', help="Logger config file or stdout (default), none to log to /dev/null")
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

    main(args.inputfile)