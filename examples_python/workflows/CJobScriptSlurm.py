r"""
Module contains the class for handling job.sh files for starting jobs on a slurm-cluster
"""
from pathlib import Path
from typing import Any, Dict, Union, List
import spinterface.constants.const_slurm as const


class CJobScriptSlurm:
    r"""
    Class for handling job.sh files for starting jobs on the cluster. Instances of this class are needed in many higher-
    level API classes as input arguments. Usually the workflow is as follows: You create an instance of this class and
    initializes this based on some given job-file on your system, then you can modify common parameters like the job-
    name with convenience functions like "adjust_jobname()". Afterward, you can write out the file to your desired
    calculation directory by just calling the instance of this class.
    """

    def __init__(self, name: str = "job.sh", sbatch_dict: Union[Dict[str, Any], None] = None,
                 modules: Union[List, None] = None, export: Union[List, None] = None, srun: Union[None,str] = None,
                 further_lines: Union[List, None] = None) -> None:
        r"""
        Initializes the Job Script handler. You can also leave the input parameters sbatch_dict, modules, export, srun
        and further lines to None and initialize this class by calling .initialize_from_file(). From a blueprint job-
        script on your file system.

        :param name: Name of the job script
        :param sbatch_dict: Key value pairs of the sbatch settings
        :param modules: List of modules to load
        :param export: List of export prompts
        :param srun: exact srun command
        :param further_lines: further lines in the job file
        """
        self._name = name
        self._interpreter = "#!/bin/bash"
        self._sbatchprefix = "#SBATCH --"
        self._sbatch_dict = sbatch_dict
        self._modules = modules
        self._export = export
        self._srun = srun
        self._further_lines = further_lines

    def adjust_jobname(self, name: str) -> None:
        r"""
        Adjusts the slurm cluster job name, which will be shown e.g. in squeue

        :param name: new name for the job
        """
        name = name + "\n"
        self._sbatch_dict[const.JOBNAME] = name

    def initialize_from_file(self, inputfilepath: Path) -> None:
        r"""
        Initializes the job script from a blueprint given by:

        :param inputfilepath: the path to the blueprint job script
        """
        file = open(inputfilepath, 'r')
        lines = file.readlines()
        try:
            self.interpreterline = lines[0]
        except ValueError:
            raise ValueError("Cannot Initialize Job Script: First Line should be Interpreter Line")
        self._sbatch_dict = {}
        self._modules, self._export, self._further_lines = [], [], []
        for line in lines[1:]:
            if line.startswith(self._sbatchprefix):
                keyval = line.split(self._sbatchprefix)[1]
                key, val = keyval.split("=")
                self._sbatch_dict[key] = val
            elif line.startswith("module load"):
                self._modules.append(line.split("module load ")[1])
            elif line.startswith("export"):
                self._export.append(line.split("export ")[1])
            elif line.startswith("srun"):
                self.srun = line
            else:
                self._further_lines.append(line)

    @property
    def interpreterline(self) -> str:
        r"""
        :return: the line describing the interpreter
        """
        return self._interpreter

    @interpreterline.setter
    def interpreterline(self, l_interpreterline: str) -> None:
        r"""
        :param l_interpreterline: sets the interpreter line (first file in the sh file)
        """
        if not l_interpreterline.startswith("#!"):
            raise ValueError("First Line (Interpreter) should start with #!")
        self._interpreter = l_interpreterline

    @property
    def srun(self) -> Union[None,str]:
        r"""
        :return: the srun command
        """
        return self._srun

    @property
    def name(self) -> str:
        r"""
        :return: the name of the job file
        """
        return self._name

    @srun.setter
    def srun(self, l_srun: Union[None,str]) -> None:
        r"""
        :param l_srun: sets the srun command
        """
        self._srun = l_srun

    def append_further_line(self, further_line: str) -> None:
        r"""
        Appends a line to the further line list:

        :param further_line: the line to append
        """
        further_line = further_line + "\n"
        if self._further_lines is None:
            self._further_lines = [further_line]
        else:
            self._further_lines.append(further_line)

    @property
    def further_lines(self) -> Union[List[str], None]:
        r"""
        :return: the further lines in the job script
        """
        return self._further_lines

    def __call__(self, where: Path = Path.cwd()) -> None:
        r"""
        Calls the writing of the job script file

        :param where: where to write the file
        """
        l_pathout = where / self._name
        with open(l_pathout, 'w') as f:
            f.write(f"{self._interpreter}")
            if self._sbatch_dict is not None:
                for key, value in self._sbatch_dict.items():
                    f.write(f"{self._sbatchprefix}{key}={value}")
            if self._modules is not None:
                for module in self._modules:
                    f.write(f"module load {module}")
            if self._export is not None:
                for export in self._export:
                    f.write(f"export {export}")
            if self._further_lines is not None:
                for further_line in self._further_lines:
                    f.write(f"{further_line}")
            if self._srun is not None:
                f.write(f"{self._srun}")
