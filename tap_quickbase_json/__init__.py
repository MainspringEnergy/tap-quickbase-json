import re

def normalize_name(name: str) -> str:
    name = name.replace('#', ' nbr ')
    name = name.replace('&', ' and ')
    name = name.replace('@', ' at ')
    name = name.replace('*', ' star ')
    name = name.replace('$', ' dollar ')
    name = name.replace('?', ' q ')
    name = str(name).strip()
    name = re.sub(r'\s+', ' ', name).lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name
