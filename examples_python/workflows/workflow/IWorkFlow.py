r"""
This module contains the abstract base class for the Spinaker workflows. The workflow is the highest API level.
"""
from spinterface.api.CSpinakerExecution import CSpinakerExecution
from spinterface.api.CJobScriptSlurm import CJobScriptSlurm
from spinterface.api.stages.IStage import IStage
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any
from copy import deepcopy
import logging as lg
import json
import pickle


class IWorkFlow(ABC):
    r"""
    Abstract base class IWorkflow. A specific implementation of a workflow is the highest level of the Spinaker API.
    A workflow is a ordered sequence of stages, while the stages are parallel executions of independent simulations.
    """

    def __init__(self, label: str, logger: Union[None, lg.Logger] = None, exe: Union[None, CSpinakerExecution] = None,
                 jobfile: Union[None, CJobScriptSlurm] = None, simudir: Union[Path, None] = None,
                 console_logging: bool = True) -> None:
        r"""
        Initializes the abstract base class for Workflows.

        :param label: The unique identifier of the Workflow instance
        :param simudir: Inputs to this are only considered if define_core_api is True. If this is None the workflow-
            simulation directory will be considered to be a folder under the defined label in the
            current working directory.
        :param logger: If this is None a default logger will be created under the name of the inheriting class
            with verbosity level: information. The logfile will be created under the name of the workflow label in the
            workflow-simulation directory.
        :param exe: Instance of CSpinakerExecution. Handles the interaction with the processes calling spinaker or
            job-scripts on a slurm cluster. If this in None the efault executable will be created which assumes
            execution on local machine as well as defined environment variable of spinaker.
        :param jobfile: This instance of the job-file can be None if calculation is performed locally.
            If the calculation shall be performed on the cluster, this has to be defined.
            If a job-file is provided this is deep-copied to avoid race-conditions.
        :param console_logging: If True the logger will also log to the console. If False only the logfile will be used.
        """
        self._label = label
        if simudir is None:
            self._simudir = Path.cwd() / f"{self._label}"
        else:
            self._simudir = simudir
        try:
            self._simudir.mkdir(exist_ok=False)
            i_mkdir_warning = False
        except FileExistsError:
            i_mkdir_warning = True
        if logger is None:
            self._logger = self._setup_logger(name=self.__class__.__name__, verbose=20,
                                              logfilepath=self._simudir / f"{self._label}.log",
                                              console_logging=console_logging)
        else:
            self._logger = logger
        if i_mkdir_warning:
            self._logger.warning("Workflow directory already exists. Will use the existing one.")
        else:
            self._logger.debug(f"Workflow directory {self._simudir} was created.")
        if exe is None:
            self._logger.debug(f"Create the default executable (local execution)...")
            self._exe = CSpinakerExecution(cluster=False)
        else:
            self._exe = deepcopy(exe)
        if jobfile is None and self._exe.cluster:
            self._logger.error("If workflow is performed on cluster provide jobfile as input.")
            raise ValueError("If workflow is performed on cluster provide jobfile as input.")
        self._jobfile = deepcopy(jobfile)
        self._stages = []

    def get_stage(self, label: str) -> IStage:
        r"""
        :param label: The unique identifier
        :returns: The stage instance associated with the label above.
        """
        for stage in self._stages:
            if stage.label == label:
                return stage

    def add_stage(self, stage: IStage, subdirname: str) -> None:
        r"""
        Adds a stage to the workflow. Overwrites the logger, the executable, the job-script to the one defined during
        initializing of the workflow. Overwrites the stage-simulation directory of the provided stage.
        This is meant to be used within an inheriting class.

        :param stage: An Instance of ISimulation. Can be an instance of any inheriting class of IStage
        :param subdirname: The name of the subdirectory for the stage. Together with the path for the workflow
            directory provided initializing this class this overwrites the stage simulation dir. set in the stage
            before adding. The sudirectory will be tried to create. If it already exists the existing one will be used
            while raising a warning.
        """
        stage.exe = self._exe
        stage.logger = self._logger
        stage.jobfile = self._jobfile
        stage.simudir = self._simudir / subdirname
        try:
            stage.simudir.mkdir(exist_ok=False)
        except FileExistsError:
            self._logger.warning(f"Dir. {subdirname} exists. Use the existing one.")
        stage.overwrite_api_all_simu()
        self._stages.append(stage)

    @property
    def n_stage(self) -> int:
        r"""
        :return: the number of registered stages.
        """
        return len(self._stages)

    @property
    def stages(self) -> List[IStage]:
        r"""
        :return: The stages registered for this workflow.
        """
        return self._stages

    @staticmethod
    def _setup_logger(name: str, verbose: int, logfilepath: Union[Path, None], console_logging: bool = True) -> lg.Logger:
        r"""
        Organizes the setup of the Logger. This method can be used by the inheriting classes to define the logger. The
        logger shall be defined on the highest API level as possible and passed through all levels. This setup function
        shall be used at the highest level to avoid duplicate code.

        :param name: The name of the logger. A good idea is to choose __name__ within the class which creates the logger
        :param verbose: The verbose-level: 10 Debug, 20: Information, 30: Warning, 40: Error
        :param logfilepath: The path defining the log-file. If None no file will be created and only the stdout will be
            used for logging.
        :return: The instance of the Logger
        """
        l_logger = lg.getLogger(name)
        if verbose in [10, 20, 30, 40]:
            formatter = lg.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            if console_logging:
                for i in range(100):
                    print("Console logging activated.")
                console_handler = lg.StreamHandler()
                console_handler.setLevel(verbose)
                console_handler.setFormatter(formatter)
                l_logger.addHandler(console_handler)
            if logfilepath is not None:
                file_handler = lg.FileHandler(logfilepath)
                file_handler.setLevel(verbose)
                file_handler.setFormatter(formatter)
                l_logger.addHandler(file_handler)
            l_logger.setLevel(verbose)
        else:
            raise ValueError("Not a valid verbose-level")
        return l_logger

    @abstractmethod
    def create(self) -> None:
        r"""
        Creates the workflow.
        """

    @property
    def logger(self) -> lg.Logger:
        r"""
        :return: The Logger of the Workflow
        """
        return self._logger

    @property
    def simudir(self) -> Path:
        r"""
        :return: the Path towards the workflow directory
        """
        return self._simudir

    @property
    def label(self) -> str:
        r"""
        :return: Unique identifier for the workflow
        """
        return self._label

    @abstractmethod
    def __call__(self, *args, **kwargs) -> None:
        r"""
        Blueprint for the call method.
        """

    def get_information(self, path_json_out: Union[Path, None] = None) -> Dict[str, Any]:
        r"""
        :param path_json_out: If a path is provided the information will be parsed to an output json.
        return: Collects the information stages and of the simulations involved in this workflow.
        """
        workflow_info_dict = {}
        for stage in self.stages:
            workflow_info_dict[stage.label] = stage.get_information()
        if path_json_out is not None:
            with open(path_json_out, "w") as f:
                json.dump(workflow_info_dict, f, indent=4)
        return workflow_info_dict

    def pickle_stage(self, label: str) -> None:
        r"""
        Pickles the information of the stage instance. Core API objects like the exe-, the logger or the jobscript are
        not included.
        """
        stage = self.get_stage(label=label)
        with open(f'{self.label}_{stage.label}.pickle', 'wb') as handle:
            pickle.dump(stage, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def unpickle_stage(self, label: str) -> None:
        r"""
        Unpickles a stage. Implicitly overwrite the API to the one set in this workflow instance. Will overwrite the
        parent directory of the pickled folder information to the current workflow simulation directory. This is needed
        if stages were calculated on another machine than the current one.

        :param label: Unique identifier for the stage.
        """
        fpath = f'{self.label}_{label}.pickle'
        with open(fpath, 'rb') as handle:
            stage = pickle.load(handle)
        stage.overwrite_parent_directory(new_parent_path=self.simudir)
        self.replace_stage(label=label, stage=stage)

    def replace_stage(self, label: str, stage: IStage) -> None:
        r"""
        Replaces the stage under stored under the corresponding label. The api will be overwritten

        :param label: Unique identifier for the stage to replace
        :param stage: Stage which be used to replace the old one.
        :raise KeyError: If label is not present.
        """
        r = -1
        for idx, l_stage in enumerate(self._stages):
            if l_stage.label == label:
                r = idx
                break
        if r == -1:
            self.logger.error(f"Key {label} not found. Cannot replace Sp. Stage.")
            raise KeyError(f"Key {label} not found. Cannot replace Sp. Stage.")
        stage.exe = self._exe
        stage.logger = self._logger
        stage.jobfile = self._jobfile
        stage.overwrite_api_all_simu()
        self._stages[r] = stage
