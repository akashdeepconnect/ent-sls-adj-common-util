from os import path
import os
this_dir = path.dirname(__file__)
functions_folder = path.join(os.path.dirname(this_dir), 'functions')
print(functions_folder)