fn add(a: Int, b: Int) -> Int:
    return a + b

fn multiply(a: Int, b: Int) -> Int:
    return a * b

fn factorial(n: Int) -> Int:
    if n <= 1:
        return 1
    return n * factorial(n - 1)