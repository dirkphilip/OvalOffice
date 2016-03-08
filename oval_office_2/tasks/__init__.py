import importlib
import os

# Import all tasks in current directory.
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or not module.endswith('.py'):
        continue
    module = ".tasks." + os.path.splitext(module)[0]
    importlib.import_module(module, package='oval_office_2')

# Make dictionary of all tasks.
from .task import Task
task_map = {i.__name__ : i for i in Task.__subclasses__()}

# Make list of all stages
stages = ['check_pre_staging', 'stage_data', 'check_post_staging', 'run', 'check_post_run']