# import Template class
from jinja2 import Template

"""
Jinja API Docs: https://bit.ly/2ZMYIyw
Template Design Docs: https://bit.ly/32GiZaI

A Template contains:
- Variables and/or Expressions: these get replaced with values when a template
is rendered;
- Tags: these control template logic;

KEEP IN MIND:

- Use "UTF-8" as Encoding for Python modules and templates because Jinja uses
UNICODE internally. That means the objects you pass to it also need to be
Unicode or ASCII characters (utf-8 encompasses all Unicode characters plus it
is also backwards compatible with ASCII).

"""

# instantiate jinja's Template Class
myTemplate = Template('{{ greeting }}, {{ name }}!')

# use Template's "render()" method along with keyword arguments or ...
print(myTemplate.render(name='Heder',greeting='Boa Tarde'))

# dictionary objects to expand the templates within a Template Class instance
print(myTemplate.render({
     'name':'Heder'
    ,'greeting':'Bom Dia'
    })
)