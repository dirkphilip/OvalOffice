import importlib
import os

for module in os.listdir(os.path.dirname(__file__)):
    if module == "__init__.py" or not module.endswith(".py"):
        continue
    module = ".systems." + module.rstrip(".py")
    importlib.import_module(module, package='oval_office_2')

from .system import System
system_map = {i.__name__: i for i in System.__subclasses__()}
