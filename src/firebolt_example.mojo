from firebolt.arrays import array
from firebolt.dtypes import int8, bool_

fn main():
    var a = array[int8](1, 2, 3, 4)
    var b = array[bool_](True, False, True)
    
    print("Created int8 array with 4 elements")
    print("Created bool array with 3 elements")
    print("Firebolt integration successful!")