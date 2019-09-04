import jsonschema
import calendar
import time


class UserError(RuntimeError):
    pass


class SentryModule:
    def __init__(self, config, cfg_schema, logger, input,
            isSource = False, isSink = False):
        if cfg_schema:
            schema_validate(config, cfg_schema,
                'pipeline item "' + config['name'] + '"')
        if 'loglevel' in config:
            logger.setLevel(config['loglevel'])
        self.input = input
        self.isSource = isSource
        self.isSink = isSink


# Convert a time string in 'YYYY-mm-dd [HH:MM[:SS]]' format (in UTC) to a
# unix timestamp
# @staticmethod
def strtimegm(str):
    for fmt in [ "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d" ]:
        try:
            return calendar.timegm(time.strptime(str, fmt))
        except:
            continue
    raise ValueError("Invalid date '%s'; expected 'YYYY-mm-dd [HH:MM[:SS]]'"
        % str)


# @staticmethod
def schema_validate(instance, schema, name):
    # "jsonschema" actually validates the loaded data structure, not the
    # raw text, so works whether the text was yaml or json.
    try:
        jsonschema.validate(instance = instance, schema = schema)
    except jsonschema.exceptions.ValidationError as e:
      if True:
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
      elif False:
        raise UserError(type(e).__name__ + ': ' + str(e))
      elif False:
        raise UserError(type(e).__name__ + ': ' + str(e.message))
      else:
        raise UserError(type(e).__name__ + ': ' +
            "\nmessage:" + str(e.message) +
            "\nschema:" + str(e.schema) +
            "\nschema_path:" + str(e.schema_path) +
            "\npath:" + str(e.path) +
            "\nabsolute_path:" + str(e.absolute_path) +
            "\ninstance:" + str(e.instance)
            )


# Convert a DBATS glob to a regex.
# Unlike DBATS, this also allows non-nested parens for aggregate grouping.
#
# From DBATS docs:
# The pattern is similar to shell filename globbing, except that hierarchical components are separated by '.' instead of '/'.
#   * matches any zero or more characters (except '.')
#   ? matches any one character (except '.')
#   [...] matches any one character in the character class (except '.')
#       A leading '^' negates the character class
#       Two characters separated by '-' matches any ASCII character between the two characters, inclusive
#   {...} matches any one string of characters in the comma-separated list of strings
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

