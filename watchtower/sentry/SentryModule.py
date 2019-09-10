"""
Base class for Watchtower Sentry modules.

Derived classes can implement a source, a filter, or a sink module.
Derived classes must implement an __init__(self, config, gen) method that
calls super().__init__(config, add_cfg_schema, logger, gen);
sources and sinks must supply an additional parameter isSource=True or
isSink=True.
Sources and filters must implement run(self) as a python generator function
that yields (key, value, time) tuples.
Filters and sinks must implement run(self) as function that reads (key, value,
time) tuples by iterating over the gen() generator.
"""

import calendar
import time
import jsonschema


def minimal_cfg_schema():
    return {
        "type": "object",
        "properties": {
            "module":   {"type": "string"}, # module name
            "loglevel": {"type": "string"}, # module loglevel
            # subclass can add more properties
        },
        "required": ["module"], # subclass can add more required properties
        # subclass can add more attributes
    }


class UserError(RuntimeError):
    pass


class SentryModule:
    def __init__(self, config, add_cfg_schema, logger, gen,
            isSource=False, isSink=False):
        if 'loglevel' in config:
            logger.setLevel(config['loglevel'])
        self.gen = gen
        self.isSource = isSource
        self.isSink = isSink
        self.modname = config['module']
        cfg_schema = minimal_cfg_schema()
        cfg_schema["additionalProperties"] = False
        if add_cfg_schema:
            for key, value in add_cfg_schema.items():
                if key == 'properties':
                    cfg_schema['properties'].update(value)
                elif key == 'required':
                    cfg_schema['required'] += value
                elif key in cfg_schema:
                    raise RuntimeError('%s attempted to modify %s attribute of '
                        'cfg_schema' % (self.modname, key))
                else:
                    cfg_schema[key] = value
        schema_validate(config, cfg_schema, 'module "' + self.modname + '" ')



# Convert a time string in 'YYYY-mm-dd [HH:MM[:SS]]' format (in UTC) to a
# unix timestamp
# @staticmethod
def strtimegm(s):
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return calendar.timegm(time.strptime(s, fmt))
        except:
            continue
    raise ValueError("Invalid date '%s'; expected 'YYYY-mm-dd [HH:MM[:SS]]'"
        % s)


# @staticmethod
def schema_validate(instance, schema, name):
    # "jsonschema" actually validates the loaded data structure, not the
    # raw text, so works whether the text was yaml or json.
    try:
        jsonschema.validate(instance=instance, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        msg = e.message
        # Some messages begin with a potentially very long dump of the
        # value of a json instance.  We strip the value, since we're going
        # to print the path of that instance.
#        instval = repr(e.instance)
#        if msg.startswith(instval) and len(instval) > 20:
#            msg = msg[len(instval):]
        path = ''.join([('[%d]' % i) if isinstance(i, int) \
            else ('.%s' % str(i)) for i in e.absolute_path])
        #raise UserError(type(e).__name__ + ' in ' + filename + ' at ' +
        raise UserError(type(e).__name__ + ' at ' +
            name + path + ': ' + msg)


# Convert a DBATS glob to a regex.
# Unlike DBATS, this also allows non-nested parens for aggregate grouping.
#
# From DBATS docs:
# The pattern is similar to shell filename globbing, except that hierarchical
# components are separated by '.' instead of '/'.
#   * matches any zero or more characters (except '.')
#   ? matches any one character (except '.')
#   [...] matches any one character in the character class (except '.')
#       A leading '^' negates the character class
#       Two characters separated by '-' matches any ASCII character between
#           the two characters, inclusive
#   {...} matches any one string of characters in the comma-separated list of
#       strings
#   Any other character matches itself.
#   Any special character can have its special meaning removed by preceeding it with '\'.
# @staticmethod
def glob_to_regex(glob):
    re_meta = '.^$*+?{}[]|()'
    glob_meta = '*?{}[]()'
    regex = '^'
    i = 0
    parens = 0
    while i < len(glob):
        if glob[i] == '\\':
            i += 1
            if i >= len(glob):
                raise UserError("illegal trailing '\\' in pattern")
            elif glob[i] not in glob_meta:
                raise UserError("illegal escape '\\%s' in pattern" % glob[i])
            elif glob[i] in re_meta:
                regex += '\\'
            regex += glob[i]
            i += 1
        elif glob[i] == '*':
            regex += '[^.]*'
            i += 1
        elif glob[i] == '?':
            regex += '[^.]'
            i += 1
        elif glob[i] == '[':
            regex += '['
            i += 1
            if i < len(glob) and glob[i] == '^':
                regex += '^.'
                i += 1
            while True:
                if i >= len(glob):
                    raise UserError("unmatched '[' in pattern")
                if glob[i] == '\\' and i+1 < len(glob):
                    regex += glob[i:i+2]
                    i += 2
                else:
                    regex += glob[i]
                    i += 1
                    if glob[i-1] == ']':
                        break
        elif glob[i] == '{':
            regex += '(?:' # non-capturing group
            i += 1
            while True:
                if i >= len(glob):
                    raise UserError("unmatched '{' in pattern")
                elif glob[i] == '\\':
                    if i+1 >= len(glob):
                        raise UserError("illegal trailing '\\' in pattern")
                    regex += glob[i:i+2]
                    i += 2
                elif glob[i] == ',':
                    regex += '|'
                    i += 1
                elif glob[i] == '}':
                    regex += ')'
                    i += 1
                    break
                elif glob[i] in '.*{}[]()':
                    raise UserError("illegal character '%s' inside {} in pattern" % glob[i])
                else:
                    if glob[i] in re_meta:
                        regex += '\\'
                    regex += glob[i]
                    i += 1
        elif glob[i] == '(':
            if parens > 0:
                raise UserError("illegal nested parentheses in pattern")
            parens += 1
            regex += '('  # capturing group
            i += 1
        elif glob[i] == ')' and parens:
            parens -= 1
            regex += ')'
            i += 1
        else:
            if glob[i] in re_meta:
                regex += '\\'
            regex += glob[i]
            i += 1
    if parens > 0:
        raise UserError("unmatched '(' in pattern")
    regex += '$'
    return regex