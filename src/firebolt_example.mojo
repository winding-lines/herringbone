from firebolt.arrays import array
from firebolt.dtypes import int8, bool_

fn main():
    var a = array[int8](1, 2, 3, 4)
    
    print("Created int8 array with 4 elements, first one: ", a.unsafe_get(0))
    print("Firebolt integration successful!")
