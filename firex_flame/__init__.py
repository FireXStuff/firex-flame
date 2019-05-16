import os

from firexapp.fileregistry import FileRegistry
from firexapp.submit.uid import Uid

FLAME_LOG_REGISTRY_KEY = 'FLAME_OUTPUT_LOG_REGISTRY_KEY'
FileRegistry().register_file(FLAME_LOG_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'flame2.stdout'))
