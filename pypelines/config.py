import os
import json
import logging
from io import IOBase

from configparser import ConfigParser, BasicInterpolation

def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy, prior of python 3.5."""
    z = x.copy()
    z.update(y)
    return z

def as_dict(config, typed=True):
    d = dict(config._sections)
    for s in d:
        d[s] = dict(config._defaults, **d[s])
        for v in d[s]:
            if typed:
                d[s][v] = get_value(config, s, v)
            else:
                d[s][v] = config.get(s, v)
    return d

def get_value(config, section, name):
    if name.endswith('_json'):
        return json.loads(config.get(section, name))
    elif name.endswith('_int'):
        return config.getint(section, name)
    else:
        return config.get(section, name)


def config2dict(config_file, interpolate=False, interpolate_envvar=[], flat=True, typed=True):
    log = logging.getLogger(__name__)
    log.info("Load configs from file:" + str(config_file))
    log.debug("interpolate=" + str(interpolate))
    if interpolate:
        if interpolate_envvar is not None:
            #this is dangerous ==> all sys env var are set up in default section
            env_var = {e:os.environ[e] for e in interpolate_envvar}
            for name in env_var:
                if any(i in env_var[name] for i in '%$'):
                    raise Exception("Environment variable " + name + " used to intrpolate has invalid characters")
            confparser = ConfigParser(defaults=env_var, interpolation=BasicInterpolation()) 
        else:
            confparser = ConfigParser(interpolation=BasicInterpolation()) 
    else:
        confparser = ConfigParser(interpolation=None)

    if isinstance(config_file, IOBase):
        #config_file is file object
        confparser.read_file(config_file)
    else:
        confparser.read(config_file)

    d = as_dict(confparser, typed=typed)
    #make it flat        
    if flat:
        flat_d = {}
        for section in d:
            for k in d[section]:
                flat_d[section + "__" + k] = d[section][k]
        d = flat_d
    else: #merge
        merge_d = {}
        for section in d:
            for k in d[section]:
                if k in merge_d:
                    raise Exception("Duplicate section key in config file " + str(config_file))
                merge_d[k] = d[section][k]
        d = merge_d
    return d


if __name__ == "__main__":
    import io
    config = io.StringIO('''
    [S1]
    a_json=[1, 2, 3]
    ''')
    d = config2dict(config)
