
#------------------------------------------------------------------------------
#   based on: https://realpython.com/python-kwargs-and-args/
#
#   1st: Unflexible summation
#------------------------------------------------------------------------------
def simpleSum(value, anotherValue):
    """
    This function returns the sum of its two arguments.
    But what if we need to sum more than two values?

    :param value: An arbitrary and numeric user input.
    :type value: numeric

    :param anotherValue: Another arbitrary and numeric user input.
    :type anotherValue: numeric
    """
    # add received argments
    return value + anotherValue

# demonstrate "simpleSum()"
print('"simpleSum()" returns: ',simpleSum(1,2))

#------------------------------------------------------------------------------
#   2nd: Flexible but cumbersome
#------------------------------------------------------------------------------
def sumList(listToSumUp):
    """
        By receiving an iterable as user input "sumList()" becomes more flexible
    than its predecessor: "simpleSum()".

        ON PYTHON ITERABLES: https://bit.ly/2NHVntE

        THIS FUNCTION'S DRAWBACK though lies in the fact a list or tuple must
    always be created for it to receive and process.

    :param listToSumUp: an iterable object containing the values to be summed up. 
    :type : tuple or list
    """
    # define "container" that'll hold summation iterations
    summation = 0
    # iterate on each list element
    for listElement in listToSumUp:
        summation += listElement
    # output result
    return summation

# execute "sumList()"
print("sumList(): returns:", sumList([1,2,3]))
print("sumList() working on a Tuple:", sumList((1,3,6)))

#------------------------------------------------------------------------------
#   3rd: use *ARGS to pass a varying number of arguments to a function
#------------------------------------------------------------------------------
def variableLengthSum(*args):
    """
        THIS FUNCTION RECEIVES any number of different positional parameters.

        The "*" means these positional ARGUMENTS ARE gonna be PACKED INTO A single
    iterable object: a TUPLE named "args" (or any other meaningful name in case
    you so desire...).

        KEEP IN MIND:
            - * (asterisk) is an UNPACKING OPERATOR;

    :param args: one or more positional arguments to be unpacked into "args" Tuple.
    :type args: 
    """
    # create cummulative summation "container"
    summation = 0
    # iterate over each element the "args" Tuple contains.
    for tupleElement in args:
        summation += tupleElement
    # output
    return summation

# demonstrate "variableLengthSum()"
print('"*args" as input for "variableLengthSum()":', variableLengthSum(1,4,8))
# unpack 3 different iterables of different length
print('Unpacking multiple iterables with "*" yields:', variableLengthSum(*[1,1,1],*(2,2,2,2),*[3,3]))


#------------------------------------------------------------------------------
# 4th: use **KWARGS to pass named/keyword arguments to a function
#------------------------------------------------------------------------------
def concatenate(**kwargs):
    """
        This function leverages the ** UNPACKING OPERATOR to assign received
    keywords arguments to a standard Python DICTIONARY.
        Keep im mind KWARGS COULD in fact BE ANY OTHER NAME.

        KEEP IN MIND:
            - ** (double asterisk) is an UNPACKING OPERATOR;

    :param kwargs: comma separated key-value pair to be unpacked into "kwargs" dictionary.
    :type kwargs: Dictionary 
    """
    # create variable holding an empty string within function scope
    stringContainer = ""
    
    #   ITERATE over "kwargs" Dictionary
    #
    #   NOTICE the Dictionary's "values()" method is used instead of iterating
    # through Dictionary keys.
    for value in kwargs.values():
        stringContainer += value
    # output results
    return stringContainer

print(concatenate(a='DOUBLE ASTERISK ** ',b='UNPACKS ',c='TO ',d='A ',e='DICTIONARY.'))

#------------------------------------------------------------------------------
# 5th: using Unpacking Operators outside "functions"
#------------------------------------------------------------------------------

# create a simple list object
simpleList = [1,2,3]
# print the list and notice its square brackets and commas
print(simpleList)

# now use the * UNPACKING OPERATOR right before the list object and then print it
# notice there are no commas nor square brackets in the output. 
print(*simpleList)

# KEEP IN MIND there can be only ONE UNPACKING OPERATION PER ASSIGNMENT
longerList = [1,2,3,4,5,6]
a, *b, c = longerList
print(f'"a" contains {a} | "b" contains {b} | "c" contains {c}.')

# use * (asterisk) to unpack and MERGE two different ITERABLES
mergedList = [*simpleList, *longerList]
# output results
print(mergedList)

# use ** (double asterisk) to MERGE DICTIONARIES
aDict = {
     'A':1
    ,'B':2
}

anotherDict = {
     'C':3
    ,'D':4
}

mergedDicts = {
     **aDict
    ,**anotherDict
}

print(mergedDicts)

# use * (asterisk) to UNPACK A STRING
# notice a list object "receives" the unpacked string contents
#
#   KEEP IN MIND:
#   the resulting variable must ALWAYS BE AN ITERABLE when the unpacking 
# operator is used within VARIABLE ASSIGNMENTS.
#
unpackedLetters = [*'Arbitrary String...']
# output results
print(unpackedLetters)






