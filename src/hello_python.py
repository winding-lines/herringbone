#!/usr/bin/env python3

from src.python_utils import greet, add_numbers

def main():
    print(greet("Bazel"))
    result = add_numbers(5, 3)
    print(f"5 + 3 = {result}")

if __name__ == "__main__":
    main()