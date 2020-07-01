#------------------------------------------------------------------------------
#   THERE ARE 4 MAJOR WAYS TO DO STRING FORMATTING IN PYTHON
#
#   Based on: https://realpython.com/python-string-formatting/
#
#   IN A NUTSHELL:
#
#   - ALWAYS handle user-supplied strings with "string" library's "Template"
# Class.
#   - PREFER "f-strings" if you're using Python 3.6 or newer
#   - USE "str.format()" if you're using an older version of Python
#   - AVOID using % (modulo) operator for string interpolation.
#
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#   1ST: THE % (MODULO) OPERATOR - "Old School" and NOT RECOMMENDED
#
#   MANDATORY READ: "printf-style" String Formatting Docs: https://bit.ly/2NH25An
#
#   EXCERPT FROM THE DOCS: "The formatting operations described here exhibit a
# variety of quirks that lead to a number of common errors (such as failing to
# display tuples and dictionaries correctly). Using the newer formatted string
# literals, the str.format() interface, or template strings may help avoid these
# errors. Each of these alternatives provides their own trade-offs and benefits
# of simplicity, flexibility, and/or extensibility."
#------------------------------------------------------------------------------

errno = 50159747054
name = 'Heder'

#   The % (modulo) operator allows simple positional string formatting
#
#   % (modulo) is also known as the "string formatting" or "interpolation"
# operator.

# KEEP IN MIND: 's' is one of the many standard conversion types.
print('"modulo" operator returns: Hello, %s' %name)

# use the 'x' conversion type to convert to hexadecimal format
print('"modulo" operator returns: hexadecimal error number is: %x' %errno)

#   MULTIPLE SUBSTITUTIONS REQUIRE A TUPLE containing the variables to be
# interpolated. 
print('"modulo" operator returns: Hey %s, there is a %x error!' %(name, errno))

#   USE VARIABLE NAMES to indicate which string is gonna be replaced by which
# variable.
#   NOTICE variable names are mapped to a Dictionary object right after the string
# to be interpolated.
print('"modulo" operator returns: Hey %(name)s, there is a %(errno)x error!' %{"name":name,"errno":errno})

#------------------------------------------------------------------------------
#   2ND: THE STR.FORMAT() METHOD (PREFERRED IN PYTHON 3+)
#
#   MANDATORY READ: "Custom String Formatting" Docs https://bit.ly/2YLGEED
#------------------------------------------------------------------------------

#   Use "format()" to do simple positional formatting.
print('"str.format()" returns: Hello, {}.'.format(name))

# VARIABLE SUBSTITUTIONS BY NAME
# NOTICE the "x" conversion type appended to "errno" below
print('"str.format()" returns: Hello, {name}, there is a {errno:x} error!'.format(name=name,errno=errno))

#------------------------------------------------------------------------------
#   3RD: F-STRINGS (Python 3.6+)
#
#   SUGGESTED READS:
#   - https://bit.ly/2VrVtdl
#   - https://realpython.com/python-f-strings/
#------------------------------------------------------------------------------

# directly embed variables without needing to call "str.format()" method
print(f'"f-string" returns: Hello, {name}.')

# NEW: f-strings allow EVALUATION OF arbitrary Python EXPRESSIONS within strings
aVar = 5
anotherVar = 10
print(f'"f-string" returns: Five plus Ten equals {aVar + anotherVar} and not {2*(aVar + anotherVar)}.')

# NOTICE it's also possible to specify different conversion types (as in "str.format")
print(f'"f-string" returns: Hello {name}, theres a {errno:x} error!')

#------------------------------------------------------------------------------
#   4TH: the "Template" CLASS within "string" LIBRARY
#   
#   "Template strings should be used when handling user-generated Strings as 
# they're safer."
#------------------------------------------------------------------------------

from string import Template

# instantiate the "Template" Class
aString = Template('Hey, $name!')

# use Template's "substitute()" method to replace values
print('"string.Template" Class returns:',aString.substitute(name=name))

# KEEP IN MIND: Template strings DO NOT SUPPORT CONVERSTION TYPES/FORMAT SPECIFIERS
errorString = 'Hey $name, there is a $error error!'

#   PASS a "normal" string to "Template" Class and directly call the "substitute()"
# method upon it.
print('"string.Template" Class returns:',Template(errorString).substitute(name=name,error=hex(errno)))

#------------------------------------------------------------------------------
#   5TH: "Template" STRINGS ARE SAFER
#
#   WATCH how a rogue user could use "str.format()" to breach our program and
# gain accesss to sensitive data.
#------------------------------------------------------------------------------

# imagine the global variable below contains sensitive info
SECRET_ACCESS_KEY='what-if-this-was-an-aws-key?'

# create a simple Class
class Error:
    def __init__(self):
        pass

#   Create a malicious string that can read data from "Global" namespace.
#   The string below will be used to access the "__globals__" dictionary
# object.
rogueUserInput = '{error.__init__.__globals__[SECRET_ACCESS_KEY]}'
# instantiate the Error Class
errorClassInstance = Error()
# use "str.format()" to leak sensitive data from our program
print(rogueUserInput.format(error=errorClassInstance))

# USE TEMPLATE STRINGS TO AVOID THIS KIND OF BREACH!
safeUserInput = '${error.__init__.__globals__[SECRET_ACCESS_KEY]}'
# NOTICE "Template" throws an "Invalid placeholder" error
Template(safeUserInput).substitute(error=errorClassInstance)








