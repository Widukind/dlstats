VERSION = (0, 7, 3)

def version_str():
    if len(VERSION) == 3: 
        return "%s.%s.%s" % VERSION
    elif len(VERSION) == 4: 
        return "%s.%s.%s-%s" % VERSION
    else:
        raise IndexError("Incorrect format for the VERSION tuple")
