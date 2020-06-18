"""----------------------------------------------------------------------------

    CLASS: BLUEPRINT

        Class is a BLUEPRINT for individual objects with exact behaviour. With
    a Class it's possible to define attributes and methods which can be reused
    by accross Class' instances. In other words, Classes allow reuse of code.


    OBJECT: EXTERNAL REF

        An instance of Class, which can perform funcionalities defined within
    Class.
        "Object" is actually how a given Class Instance is referenced "OUTSIDE"
    of itself (outside of its definition code).


    SELF: INTERNAL REF
    
        Represents the instance of Class within Class definition. "self" is
    used to access both a Classes' methods and attributes.
        "self" is how a Class instance is referenced to "INSIDE" within itself
    (inside of its definition code).


    __INIT__: CONSTRUCTOR/"BINDER"

        A reserved method in Python Classes, it's called when an Object is
    created/derived from Class.
        "__init__" allows the Class to initialize all of its methods and 
    attributes. In object oriented concepts, it is known as a CONSTRUCTOR.
        Docs available here https://bit.ly/2N9F1tu


----------------------------------------------------------------------------"""

#------------------------------------------------------------------------------
# Example 1: Use Cars to demonstrate Python Classes usage:
#------------------------------------------------------------------------------

class Car(object):
    """
        Blueprint for building a car.
    """
    # first of all, define the "__init__" method
    def __init__(
         self
        ,model
        ,color
        ,maker
        ,max_speed
    ):
        #   ASSIGN received arguments to its corresponding ATTRIBUTES: use 
        # "self" to perform this binding operation.
        self.model=model
        self.color=color
        self.maker=maker
        self.max_speed=max_speed

    #   INDENTATION MATTERS!!!   
    #   Notice all methods defined below have the same indentation as __init__
    def start_engine(self):
        print("engine started.")

    def stop_engine(self):
        print("engine stopped.")

    def accelerate(self):
        print("accelerating...")
        # additional acceleration functionality should be added here

    def change_gear(self):
        print("gear changed")
        # gear related functionality should be added here

# Create two different Car instances
fuscao_preto = Car(
     model='Fusca'
    ,color='Black'
    ,maker='VolksWagen'
    ,max_speed=120
)

# check fuscao_preto's type
print('fuscao_preto is of type: ',type(fuscao_preto))

variant_amarela = Car(
     model='Variant'
    ,color='Yellow'
    ,maker='VolksWagen'
    ,max_speed=60
)

# access variant_amarela's Object "max_speed" attribute
print("variant_amarela's Max Speed is:", variant_amarela.max_speed,"mph.")
# call variant_amarela's "change_gear()" method
variantMessageToTheWorld = variant_amarela.change_gear()

print(f'variant_amarela says:"',variantMessageToTheWorld,'".')

#------------------------------------------------------------------------------
#   Example 2: Calculate the Area of a Rectangle
#
#   Imagine you need to calculate the cost to build a rectangle with varying
# measures.
#------------------------------------------------------------------------------

class Rectangle(object):

    # First of all, define __init__ method
    def __init__(
         self
        ,length
        ,breadth
        # "unit_cost" has zero as its default value
        ,unit_cost=0
    ):
        # Second, define attributes
        self.length=length
        self.breadth=breadth
        self.unit_cost=unit_cost

    # Third, define Class's methods
    def get_perimeter(self):
        return 2 * (self.length + self.breadth)

    def get_area(self):
        return self.length * self.breadth

    def calculate_cost(self):
        # create auxiliary "area" variable
        area = self.get_area()
        return area * self.unit_cost

# Exercise: calculate the Area and Cost to build the specified rectangle:
# length: 160 cm
# breadth: 120 cm
# Unit Cost (per Square cm): USD 2.000,00

# First, instantiate (create Object) from "Rectangle" Class
retangulo=Rectangle(
     length=160
    ,breadth=120
    ,unit_cost=2000
)

# store results in variables
resultingArea = retangulo.get_area()

costToBuild = retangulo.calculate_cost()

print(f'"retangulo" object area:',resultingArea,'cm.')

print(f'"retangulo" construction cost:',costToBuild,'USD.')