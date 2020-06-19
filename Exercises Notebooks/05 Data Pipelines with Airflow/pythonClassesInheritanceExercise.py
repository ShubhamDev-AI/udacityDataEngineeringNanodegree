
"""----------------------------------------------------------------------------
    STUDIES OF PYTHON'S "super()" FUNCTION FOR SINGLE INHERITANCE IN CLASSES

    The exercises below are based on this article: https://bit.ly/2Cn8AFZ
----------------------------------------------------------------------------"""

#------------------------------------------------------------------------------
# Example 1: Create Two Distinct Classes with similar methods between them
#------------------------------------------------------------------------------

class Rectangle:
    
    # First of all, define "__init__"
    def __init__(self,length,width):
        self.length=length
        self.width=width

    def area(self):
        # calculate Rectangle's area
        return self.length * self.width

    def perimeter(self):
        # calculate Rectangle's perimeter
        return 2 * self.length + 2 * self.width

class Square:

    # 1st: define "__init__"
    def __init__(self,length):
        self.length=length

    def area(self):
        # calculate area
        return self.length * self.length

    def perimeter(self):
        # calculate perimeter
        return 4 * self.length

# Use both Classes' methods by:
#   - first instantiating them (create Objects)
#   - call a method from each

quadrado = Square(4)
print("quadrado's Area equals:",quadrado.area())

retangulo = Rectangle(2, 4)
print("retangulo's Area equals:",retangulo.area())


#------------------------------------------------------------------------------
#   Use Python's "super()" function to create a Classe that inherits methods
# from other Class.
#------------------------------------------------------------------------------

class NewRectangle:
    # You know what to do, right? "__init__" it!
    def __init__(self, length, width):
        self.length=length
        self.width=width

    def area(self):
        return self.length * self.width

    def perimeter(self):
        return 2 * self.length + 2 * self.width

# "NewSquare" Class inherits all of "NewRectangle"'s methods
# ATTENTION: "super()" is a function, remember its parenthesis
class NewSquare(NewRectangle):
    # Gotta "__init__"'Em All
    def __init__(self,length):
        # Notice both "width" AND "length" attributes are initialized equally
        # with "length"'s value;
        super().__init__(length,length)

# Instantiate "NewSquare" and call one of its methods
quadrado_herdado = NewSquare(4)
print("quadrado_herdado's Area equals:",quadrado_herdado.area())

# create a "cube" Class which inherits its methods from "NewSquare"
class Cube(NewSquare):

    #   IMPORTANT
    #
    #   "Cube" doesn't have an "__init__" method because __init__ wouldn't do
    # anything different than what already happens in "NewSquare", from which
    # it inherits everything.
    #   In this situation, its superclass's "__init__" method is then 
    # automatically called.
    #   "super()" then returns a delegate object to the parent Class, which
    # allows you to call its methods directly.

    def surface_area(self):
        # use "super()" to access NewSquare's "area()" method
        face_area = super().area()
        # calculate Face Area
        return face_area * 6

    def volume(self):
        # one more time "super()" comes to the rescue
        face_area = super().area()
        # calculate volume
        return face_area * self.length

# instantiate Cube Class
cubo = Cube(3)
print("cubo's Surface Area equals:",cubo.surface_area())
print("cubo's Volume equals:",cubo.volume())