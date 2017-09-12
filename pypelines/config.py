import os
import json
import logging
from io import IOBase

from configparser import ConfigParser, BasicInterpolation


# def as_dict(self):
#     d = dict(self._sections)
#     for k in d:
#         d[k] = dict(self._defaults, **d[k])
#         d[k].pop('__name__', None)
#     return d


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
        return json.dumps(config.get(section, name))
    elif name.endswith('_int'):
        return config.getint(section, name)
    else:
        return config.get(section, name)


def config2dict(config_file, interpolate=False, interpolate_envvar=False, flat=True):
    log = logging.getLogger(__name__)
    log.info("Load configs from file:" + str(config_file))
    
    if interpolate:
        if interpolate_envvar:
            #this is dangerous ==> all sys env var are set up in default section
            confparser = ConfigParser(defaults=os.environ, interpolation=BasicInterpolation()) 
        else:
            confparser = ConfigParser(interpolation=BasicInterpolation()) 
    else:
        confparser = ConfigParser(interpolation=None)

    if isinstance(config_file, IOBase):
        #config_file is file object
        confparser.read_file(config_file)
    else:
        confparser.read(config_file)

    d = as_dict(confparser)
    
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
    
    
    # confparser.read(config_file)
    # sections = confparser.sections()
    # sections.append('DEFAULT')
    # variables_conffile_all_sections = {s:dict(confparser.items(s)) for s in sections}
    # #variables_cmdline = {**variables_cmdline, **dict(confparser.items('DEFAULT'))} #default overwrite variables_cmdline, py35 only
    # #variables_cmdline = merge_two_dicts(variables_cmdline, **dict(confparser.items('DEFAULT')))
    # #log.info("Params found in files: " + str(variables_cmdline))
    # return variables_conffile_all_sections



if __name__ == "__main__":
    import io
    config = io.StringIO('''
    [s1]
    a=b
    c=d
    ''')
    d = config2dict(config, flat=False)
