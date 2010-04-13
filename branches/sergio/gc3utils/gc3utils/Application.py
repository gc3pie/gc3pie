import types
# -----------------------------------------------------
# Applications
#

class Application():
    def __init__(self, application_tag, cores, memory_per_core, walltime, input_list, application_arguments_list, output_list):
        self.application_tag = application_tag
        self.cores = cores
        self.memory_per_core = memory_per_core
        self.walltime = walltime
        self.input_list = input_list
        self.application_arguments_list = application_arguments_list
        self.output_list = output_list


