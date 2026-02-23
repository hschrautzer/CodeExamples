r"""
This module contains the abstract base class for the implementation of Stages
"""
from abc import ABC, abstractmethod
from pathlib import Path
import logging as lg
from spinterface.api.CSpinakerExecution import CSpinakerExecution
from spinterface.api.CJobScriptSlurm import CJobScriptSlurm
from typing import Union, List, Dict, Any
from copy import deepcopy
from spinterface.api.simulations.ISimulation import ISimulation
import time
import pandas as pd


class IStage(ABC):
    r"""
    Abstract base class for implementation of stages. A stage is meant to be a collection of independent ISimulation
    instances, which shall be executed in parallel. They could also be started sequentially (of course with loss of
    efficiency). The important aspect is that these Simulations do not depend on each other. This class provides methods
    for starting all the simulations and waiting until all of them are finished.
    """

    def __init__(self, label: str, define_core_api: bool = True, logger: Union[None,lg.Logger]= None,
                 exe: Union[None,CSpinakerExecution]=None, jobfile: Union[None, CJobScriptSlurm] = None,
                 simudir: Union[Path, None] = None) -> None:
        r"""
        Initializer for abstract base class for Spinaker Stages

        :param label: Unique identifier for the instance of the stage
        :param define_core_api: Flag if logger, exe, jobfile and stage-simulation directory are defined during
            initialization of this class. If you use an implementation of this class directly it is recommended to
            define all of them here and set this flag to True (Default). If the instance of this class is used within
            a workflow it is recommended to set this to False. The core API will then be defined with the settings of
            the workflow, respectively.
        :param simudir: Inputs to this are only considered if define_core_api is True. If this is None the stage-
            simulation directory will be considered to be a folder under the defined label in the
            current working directory.
        :param logger: Inputs to this are only considered if define_core_api is True. If this is None a default logger
            will be created under the name of the inheriting class with verbosity level: information. The logfile will
            be created under the name of the stage label in the stage-simulation directory.
        :param exe: Inputs to this are only considered if define_core_api is True. Instance of CSpinakerExecution.
            Handles the interaction with the processes calling spinaker or job-scripts on a slurm cluster.
            For race condition avoiding if this is used in a workflow the instance is deep-copied. If this in None the
            default executable will be created which assumes execution on local machine as well as defined environment
            variable of spinaker.
        :param jobfile: Inputs to this are only considered if define_core_api is True. This instance of the job-file
            can be None if calculation is performed locally. If the calculation shall be performed on the cluster,
            this has to be defined. If a job-file is provided this is deep-copied to avoid race-conditions.
        :raise ValueError: If define_core_api is True AND jobfile is None AND exe.cluster is True.
        """
        self._label = label
        self._define_core_api = define_core_api
        if self._define_core_api:
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
                                                  logfilepath=self._simudir / f"{self._label}.log")
            else:
                self._logger = logger
            if i_mkdir_warning:
                self._logger.warning("Stage directory already exists. Will use the existing one.")
            else:
                self._logger.debug(f"Stage directory {self._simudir} was created.")
            if exe is None:
                self._logger.debug(f"Create the default executable (local execution)...")
                self._exe = CSpinakerExecution(cluster=False)
            else:
                self._exe = deepcopy(exe)
            if jobfile is None and self._exe.cluster:
                self._logger.error("If stage is performed on cluster provide jobfile as input.")
                raise ValueError("If stage is performed on cluster provide jobfile as input.")
            self._jobfile = deepcopy(jobfile)
        else:
            self._simudir = None
            self._exe = None
            self._logger = logger
            self._jobfile = None
        self._simulations = []
        self._subdirnames = []
        self._ready = False

    @staticmethod
    def _setup_logger(name: str, verbose: int, logfilepath: Union[Path, None]) -> lg.Logger:
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

    @property
    def simulations(self) -> List[ISimulation]:
        r"""
        :return: A list of the simulation instances included in this stage
        """
        return self._simulations

    def add_simulation(self, simulation: ISimulation, subdirname: str) -> None:
        r"""
        Adds a simulation instance to the list of simulations. Overwrites the logger, the executable, the job-script
        to the one defined during initializing of the stage. Overwrites the simulations directory of the provided
        simulations. This is meant to be used within an inheriting class.

        :param simulation: An Instance of ISimulation. Can be an instance of any inheriting class of ISimulation
        :param subdirname: The name of the subdirectory for the simulations. Together with the path for the simulation
            directory provided initializing this class this overwrites the simulationsdirectory set in the simulation
            before adding. The sudirectory will be tried to create. If it already exists the existing one will be used
            while raising a warning.
        """
        if self._define_core_api:
            simulation.exe = self._exe
            simulation.logger = self._logger
            simulation.jobfile = self._jobfile
            simulation.simudir = self._simudir / subdirname
            try:
                simulation.simudir.mkdir(exist_ok=False,parents=True)
            except FileExistsError:
                self._logger.warning(f"Dir. {subdirname} exists. Use the existing one.")
        self._subdirnames.append(subdirname)
        self._simulations.append(simulation)

    def get_simulation(self, label: str) -> Union[None,ISimulation]:
        r"""
        :param label: the unique identifier for the targeted simulation. If not found None will be returned.
        """
        for simulation in self.simulations:
            if simulation.label == label:
                return simulation
        return None

    def overwrite_api_all_simu(self) -> None:
        r"""
        This function is needed if the instance of the stage is used within a workflow (and so define_core_api is set
        to False for this stage). After adding this stage to a workflow (which will assign the workflow api to the stage
        api), use this function to assign all of the simulations the api passed through by the workflow instance.
        """
        if self._define_core_api:
            self.logger.warning("Overwriting API for all simulations has no effect when define_core_api is True for"
                                "this stage.")
            return
        for idx,simulation in enumerate(self.simulations):
            simulation.exe = self._exe
            simulation.logger = self._logger
            simulation.jobfile = self._jobfile
            simulation.simudir = self._simudir / self._subdirnames[idx]
            try:
                simulation.simudir.mkdir(exist_ok=False, parents=True)
            except FileExistsError:
                self._logger.warning(f"Dir. {self._subdirnames[idx]} exists. Use the existing one.")

    @property
    def subdirname(self) -> str:
        r"""
        :return: The last part of the path to the stage simulation directory. This is the dir. name for the stage.
        """
        return str(self.simudir.parts[-1])

    @property
    def n_simulation(self) -> int:
        r"""
        :return: the number of registered simulations
        """
        return len(self._simulations)

    def __call__(self, walltime: Union[float, None] = None, check_time_interval: float = 1,
                 cluster_parallel: bool = True, local_parallel: bool = True) -> None:
        r"""
        Starts all simulations of the stage. If calculation is performed on local machine the python process will be
        blocked until the end of the Spinaker process (unless the variable local_block_python is False).
        This blocks the server-client parallelism on the local machine to avoid race - conditions.
        If calculation is performed on the cluster the default setting is that many processes (in terms of
        sending batch-scripts) can be spawned without blocking the python process. You can force the blocking behaviour
        on the cluster by the flag: cluster_block_python.

        :param cluster_block_python: The Default to this is False. If active the python process is blocked until the end
            of the simulation on the cluster.
        :param local_block_python: The Default to this is True. If not active the python process is not blocked and
            multiple spinaker simulations can be spawned. Be careful: if this is False you probably have to wait
            elsewhere. Since if the python process ends before spinaker came to an end the child process (spinaker) will
            also be killed.
        :param walltime: Walltime used for local machine or used on cluster if cluster_block_python is active.
        :param check_time_interval: Time intervall used for checking end of simulation on local machine or on cluster if
            cluster_block_python is active.
        :raise TimoutError: If sending of batch script on the cluster exceeds 10s a TimeoutError will be raised. Can
            be catched outside for error handling of this specific simulation. Also a TimeoutError will be raised if
            waiting for simulation end exceeds the walltime (this is also true if cluster_block_python is active)
        """
        self._logger.info(f"Start all simulations...")
        for simulation in self.simulations:
            simulation(walltime=walltime, check_time_interval=check_time_interval,
                       cluster_block_python=not cluster_parallel, local_block_python=not local_parallel)
        self._logger.info("...done")

    def wait_for_stage_end(self, walltime: Union[float, None] = None, check_time_interval: float = 10,
                           print_progessbar = False) -> None:
        r"""
        A stage is thought to be a container for not dependent simulations. Calling this instance leads (if executed
        with default arguments) to a parallel execution of the registered simulations. This function waits until all
        of these simulations are finished.

        :param walltime: Maximum time. If exceed a timeout-error will be raised. If None the walltime is infinite.
        :param check_time_interval: The time intervall in which it is tested whether all simulations are performed. The
            time is given in seconds.
        :raise TimeOutError: Will be raiesd if walltime is exceeded. Can be catched outside for timeout error handling.
        """
        l_total_time = 0
        while not all([simu.exe.check_sim_end() for simu in self.simulations]):
            print("Waiting for all simulations to be ready...")
            print(f"Number of finished simulations: {sum([simu.exe.check_sim_end() for simu in self.simulations])}/"
                  f"{len(self.simulations)}")
            text = f"Number of finished simulations: {sum([simu.exe.check_sim_end() for simu in self.simulations])}/"f"{len(self.simulations)}"
            self.logger.info(text)
            time.sleep(check_time_interval)
            l_total_time += check_time_interval
            if walltime is not None:
                if l_total_time >= walltime:
                    raise TimeoutError
        for simu in self.simulations:
            simu.check_spk_completed()
        if print_progessbar:
            n_elements = len(self.simulations)
            j = sum([simu.ready for simu in self.simulations])
            x = int(n_elements * j / n_elements)
            print(f" Waiting for Stage [{u'█' * x}{('.' * (n_elements - x))}] {j}/{n_elements}", end='\r', flush=True)
        self._logger.info(f"All simulations of stage {self._label} ready.")
        self._ready = True

    @property
    def label(self) -> str:
        r"""
        :return: the unique identifier for the stage
        """
        return self._label

    @property
    def simudir(self) -> Path:
        r"""
        :return: The Path to the stage simulation directory
        """
        return self._simudir

    @simudir.setter
    def simudir(self, new_simudir: Path) -> None:
        r"""
        :param new_simudir: Overwrites the stage simulation directory
        """
        self._simudir = new_simudir

    @property
    def exe(self) -> CSpinakerExecution:
        r"""
        :return: the instance of CSpinakerExecution
        """
        return self._exe

    @exe.setter
    def exe(self, new_exe: CSpinakerExecution) -> None:
        r"""
        :param new_exe: Overwrite the executable instance. For avoiding race-dependencies this is deepcopied
        """
        self._exe = deepcopy(new_exe)

    @property
    def jobfile(self) -> Union[None, CJobScriptSlurm]:
        r"""
        :return: the instance of the CJobScriptSlurm class
        """
        return self._jobfile

    @jobfile.setter
    def jobfile(self, new_jobfile: CJobScriptSlurm) -> None:
        r"""
        :param new_jobfile: Overwrites the jobfile. A Jobfile has to be present in exe.cluster is True.
        """
        self._jobfile = deepcopy(new_jobfile)

    @property
    def logger(self) -> lg.Logger:
        r"""
        :return: The instance of the Logger
        """
        return self._logger

    @logger.setter
    def logger(self, new_logger: lg.Logger) -> None:
        r"""
        Registers the logger for the instance of ISimulation.

        :param new_logger: Overwrites the current logger.
        """
        self._logger = new_logger

    @property
    def ready(self) -> bool:
        r"""
        :return: If the stage is ready.
        """
        return self._ready

    def get_information(self) -> Union[None,Dict[str,Any]]:
        r"""
        :return: Collects the information of the simulations involved in this stage. If stage is not yet ready -> None.
        """
        stage_info_dict = {}
        for simulation in self.simulations:
            stage_info_dict[simulation.label] = simulation.get_information()
        return stage_info_dict

    def __getstate__(self):
        r"""
        This method is used for pickle stage objects. Since e.g. the logger contains a thread loc. it can't be
        pickled. Thus parts of the core api are not considered for the pickle-process.
        """
        return {key: (value if key not in ["_exe", "_logger", "_jobfile"] else None)
                for (key, value) in self.__dict__.items()}

    def overwrite_parent_directory(self, new_parent_path: Path) -> None:
        r"""
        This routine is needed for realizing a workflow on different machines. When e.g. unpickling a stage which was
        calculated at the cluster on the local machine we need to reference all path information of the stage as well
        as the containing simulations to the new directory of the workflow folder. This means overwriting the parent
        paths.

        :param new_parent_path: Path to the new parent directory.
        """
        l_sd_name = self.subdirname
        self.simudir = new_parent_path / l_sd_name

