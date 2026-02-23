r"""
This module contains the abstract base class ISimulation, which is the blueprint for the API to the individual
algorithms spinaker provides.
"""
from abc import abstractmethod, ABC
from spinterface.api.CSpinakerExecution import CSpinakerExecution
from spinterface.api.CWriteInputs import CWriteInputs
from spinterface.api.CJobScriptSlurm import CJobScriptSlurm
from spinterface.constants.const_spinaker import SPINAKER_FINISH_LINE
from spinterface.constants.const_spinaker_filenames import SPK_OUT_GENERAL_INFO
from typing import Union, Dict, Any
from copy import deepcopy
import json
import logging as lg
from pathlib import Path


class ISimulation(ABC):
    r"""
    Abstract class for calling a Spinaker Algorithm. The inheriting classes will specify the details concerning the
    concrete choice of the algorithm.
    """

    def __init__(self, label: str, define_core_api: bool = True, logger: Union[lg.Logger, None] = None,
                 exe: Union[None, CSpinakerExecution] = None, jobfile: Union[None, CJobScriptSlurm] = None,
                 simudir: Union[Path, None] = None, **kwargs: CWriteInputs) -> None:
        r"""
        Prescribe the initialization of a calculation instance.

        :param label: Unique label identifying the simulation instance
        :param define_core_api: Flag if logger, exe, jobfile and simulation directory are defined during initialization
            of this class. If you use an implementation of this class directly it is recommended to define all of them
            here and set this flag to True (Default). If the instance of this class is used within a workflow or a stage
            it is recommended to set this to False. The core API will then be defined with the settings of the stage or
            workflow, respectively.
        :param simudir: Inputs to this are only considered if define_core_api is True. If this is None the simulation
            directory will be considered to be a folder under the defined label in the current working directory.
        :param logger: Inputs to this are only considered if define_core_api is True. If this is None a default logger
            will be created under the name of the inheriting class with verbosity level: information. The logfile will
            be created under the name of the simulation label in the simulation directory.
        :param exe: Inputs to this are only considered if define_core_api is True. Instance of CSpinakerExecution.
            Handles the interaction with the processes calling spinaker or job-scripts on a slurm cluster.
            For race condition avoiding if this is used in a workflow the instance is deep-copied. If this in None the
            default executable will be created which assumes execution on local machine as well as defined environment
            variable of spinaker.
        :param jobfile: Inputs to this are only considered if define_core_api is True. This instance of the job-file
            can be None if calculation is performed locally. If the calculation shall be performed on the cluster,
            this has to be defined. If a job-file is provided this is deep-copied to avoid race-conditions.
        :param \**kwargs: Key-Value pairs: The keys are strings containing the name of the spinaker inputfile
            (of course without the `.json` ending). The values are the corr. instances of the CWriteInputs class. Note,
            that this won't be tested.
        :raise ValueError: If define_core_api is True AND jobfile is None AND exe.cluster is True.
        """
        self._label = label
        if define_core_api:
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
                self._logger.debug(f"Simulation directory {self._simudir} was created.")
            if exe is None:
                self._logger.debug(f"Create the default executable (local execution)...")
                self._exe = CSpinakerExecution(cluster=False)
            else:
                self._exe = deepcopy(exe)
            self._exe.calcdir = self._simudir
            if jobfile is None and self._exe.cluster:
                self._logger.error("If calculation is performed on cluster provide jobfile as input.")
                raise ValueError("If calculation is performed on cluster provide jobfile as input.")
            self._jobfile = deepcopy(jobfile)
            if jobfile is not None:
                self._jobfile.adjust_jobname(name=self._label)
        else:
            self._simudir = None
            self._exe = None
            self._logger = None
            self._jobfile = None
        # Parse the input files
        self._inputfiles = dict(**kwargs)
        self._spk_logfile_name = self._check_general_spk_input()
        self._ready = False
        self.check_spk_completed()

    @staticmethod
    def _setup_logger(name: str, verbose: int, logfilepath: Union[Path, None]) -> lg.Logger:
        r"""
        Organizes the setup of the Logger.

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

    def _check_general_spk_input(self) -> str:
        r"""
        Make sure that a general.json defines properly the logfile output of spinaker. This is needed to evaluate if the
        spinaker evaluation actually completed with the proper finish line.

        :return: The name of the spinaker logging file
        :raise FileNotFoundError: If general.json is not found within the input files.
        """
        if not "general.json" in [ifile.name for ifile in self._inputfiles.values()]:
            self.logger.error("Cannot find general.json input file.")
            raise FileNotFoundError
        general_input = self._inputfiles["general"]
        # Make sure a logfile is written
        general_input.adjust_parameter(key="i_logfile", value=True)
        # Make sure the parameter logfilename is present and readout the name
        try:
            lf = general_input.get_parameter(key="logfilename")
        except KeyError:
            lf = "spk_logfile.txt"
            general_input.insert_parameter(key="logfilename", value=lf)
        return lf

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
    def label(self) -> str:
        r"""
        return: the unique identifier for the simulation instance
        """
        return self._label

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
        if new_jobfile is not None:
            self._jobfile.adjust_jobname(name=self._label)

    @property
    def simudir(self) -> Path:
        r"""
        :return: The directory for the simulation. I
        """
        return self._simudir

    @simudir.setter
    def simudir(self, new_simudir: Path) -> None:
        r"""
        :param new_simudir: Overwrites path to simulation directory. Also overwrites the calculation directory in the
            executable to be sure that both point to the same directory.
        """
        self._simudir = new_simudir
        self.exe.calcdir = new_simudir

    @property
    def inputfiles(self) -> Dict[str, CWriteInputs]:
        r"""
        :return: A dictionary containing the name of the inputfiles (keys) and the corresponding instances of the API
            class CWriteInputs (values).
        """
        return self._inputfiles

    @property
    def started(self) -> bool:
        r"""
        :return: whether the calculation was started
        """
        return self._started

    @started.setter
    def started(self, l_started: bool) -> None:
        r"""
        :param: whether the calculation is started
        """
        self._started = l_started

    @property
    def ready(self) -> bool:
        r"""
        :return: If the calculation is ready. For the evaluation the check_sim_end method (CSpinakerExecution) is used.
        """
        return self.exe.i_ready

    @property
    def spinaker_completed(self) -> bool:
        r"""
        :return: If Spinaker execution is completed. Sometimes the execution stops with an internal spinaker error. This
            results in evaluating the .ready property to True. It is checked here if the full spinaker calculation
            completed with printing the proper finish line to the logfile.
        """
        return self._spk_completed

    def check_spk_completed(self) -> None:
        r"""
        If Spinaker execution is completed. Sometimes the execution stops with an internal spinaker error. This
        results in evaluating the .ready property to True. It is checked here if the full spinaker calculation
        completed with printing the proper finish line to the logfile. Will set the property spk_completed.
        """
        try:
            with open(self.simudir / self._spk_logfile_name, "r") as spk_logfile:
                for line in spk_logfile:
                    pass
                last_line = line
            if last_line.endswith(SPINAKER_FINISH_LINE + '\n'):
                self._spk_completed = True
            else:
                self._spk_completed = False
        except Exception:
            self._spk_completed = False

    def __call__(self, walltime: Union[float, None] = None, check_time_interval: float = 1,
                 cluster_block_python: bool = False, local_block_python: bool = True) -> None:
        r"""
        Starts the simulation. If calculation is performed on local machine the python process will be blocked until the
        end of the Spinaker process (unless the variable local_block_python is False).
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
        if self._exe.cluster:
            self._jobfile.adjust_jobname(name=self.label)
            self._jobfile(where=self._simudir)
            self.exe.path_exe =  self._simudir / self._jobfile.name
            self.exe()  # Send the job (sbatch...)
            self._logger.info(f"Started simulation {self._label} on the cluster...")
            try:
                self.exe.wait_process_end(walltime=10,
                                          check_time_interval=0.1)  # sending of the process should be quick
            except TimeoutError:
                self.logger.error(f"Couldn't start {self._label} (Timeout).")
                raise TimeoutError
            if self.exe.errors != "":
                self._logger.error(self.exe.errors)
            self._logger.info(f"Assigned JobID: {self.exe.job_id_cluster}.")
            self._started = True
            if cluster_block_python:
                # Wait for simulation to end
                try:
                    self.exe.wait_to_sim_end(walltime=walltime, check_time_interval=check_time_interval)
                except TimeoutError:
                    self.logger.error(f"Walltime exceeded for waiting for sim. end for: {self._label}")
                self._logger.info(f"Completed simulation {self._label} on the cluster (time: {self.total_time}s).")
                self.check_spk_completed()
        else:
            self.exe()  # Start the spinaker process
            self._logger.info(f"Started simulation {self._label} on the local machine...")
            self._started = True
            if local_block_python:
                try:
                    self.exe.wait_to_sim_end(walltime=walltime, check_time_interval=check_time_interval)
                    # Wait till the end of the simulation
                except TimeoutError:
                    self.logger.error(f"Walltime exceeded for waiting for sim. end for: {self._label}")
                # the simulation before continuing.
                if self.total_time is None:
                    self._logger.warning(f"Completed simulation {self._label} on local machine but time is None.")
                    self._logger.warning(f"It is likely that there was an error during spinaker execution.")
                else:
                    self._logger.info(
                        f"Completed simulation {self._label} on local machine (time: {self.total_time}s).")
                    self.check_spk_completed()

    @abstractmethod
    def write_simulation_inputs(self) -> None:
        r"""
        Prescribes writing of simulation inputs.
        """

    @property
    def total_time(self) -> Union[float, None]:
        r"""
        :return: The total time measured by spinaker and read from the general_info.json.
        """
        try:
            with open(self.simudir / SPK_OUT_GENERAL_INFO, "r") as f:
                data = json.load(f)
            return float(data["total_time"])
        except FileNotFoundError:
            return None

    @property
    def spk_logfile_name(self) -> str:
        r"""
        :return: The name of the spinaker logfile
        """
        return self._spk_logfile_name

    def get_meta_information(self) -> Dict[str, Any]:
        r"""
        This routine can be used from any of the inheriting classes to get the meta information. This dictionary can
        then be expanded with the specific simulation outputs.

        :return: A dictionary containing meta information like the simulation time.
        """
        return {"label": self._label,"ready": self.ready, "spk_completed": self.spinaker_completed, "total_time": self.total_time}

    @abstractmethod
    def get_information(self) -> Union[None, Dict[str, Any]]:
        r"""
        Blueprint function for inheriting classes.
        """

    def __getstate__(self):
        r"""
        This method is used for pickle simulation objects. Since e.g. the logger contains a thread loc. it can't be
        pickled. Thus parts of the core api are not considered for the pickle-process.
        """
        return {key: (value if key not in ["_exe", "_logger", "_jobfile"] else None)
                for (key, value) in self.__dict__.items()}
    @property
    def subdirname(self) -> str:
        r"""
        :return: The last part of the path to the simulation directory. This is the dir. name for the stage.
        """
        return str(self.simudir.parts[-1])

