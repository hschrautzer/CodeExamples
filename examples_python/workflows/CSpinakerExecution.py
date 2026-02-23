# -*- coding: utf-8 -*-
r"""
Module contains class responsible for execution of Spinaker
"""
from pathlib import Path
from typing import Union
import platform
from spinterface.constants.const_paths import SPINAKER_EXE
from spinterface.constants.const_spinaker import SPINAKER_EXE_NAME
from spinterface.constants.const_slurm import PENDING, COMPLETED, TIMEOUT, FAILED, OUT_OF_MEMORY
from spinterface.api.pathutilities import change_directory
import subprocess
import logging as lg
import time


class CSpinakerExecution:
    """
    Responsible for execution of Spinaker. Either on local machine or via JobScript on a Slurm cluster. On a local
    machine the recommended workflow is:\n
    - Create runner: exe = CSpinakerExecution(...)\n
    - Start process: exe()...\n
    - Wait for simulation to end: exe.wait_to_sim_end()\n
    On a cluster the recommended workflow is:\n
    - Create runne: exe = CSpinakerExecution(...)\n
    - Start process: exe()...\n
    - Wait for process end of sbatch sending: exe.wait_process_end(...)\n
    - If you want to block the python process until the simulation ends: exe.wait_to_sim_end(...)\n
    - If you want to continue the python process don't do the above. Instead, you can check if the simulation ended with:
        exe.check_sim_end(...)\n
    You can also do the cluster workflow on your local machine. This just produces the overhead of twice checking if the
    simulation/process is ended.
    """

    def __init__(self, path_exe: Union[Path, None] = None, cluster: bool = False, calcdir: Union[None, Path] = None,
                 logger: Union[lg.Logger, None] = None) -> None:
        r"""
        Initializes the execution class. This instance is able to call Spinaker on a local machine and on
        a Slurm cluster via a Jobscript.

        :param path_exe: Path to spinaker executable or to job-script. If None this will be initialized according to the
            value of cluster. If cluster is False and path_exe is None it is assumed that an environment variable
            SPINAKER is defined, which contains the path to your spinaker executable. If cluster is True and path_exe
            None the jobscript is expected to be in the calculation directory and named job.sh.
        :param cluster: boolean whether we are on a slurm cluster or on a local machine
        :param calcdir: Directory in which the calculation shall be performed. If this is None it will be set to the
            current working directory.
        :param logger: A logging instance. If None logging will be send to stdout.
        """
        self._logger = logger
        self._cluster = cluster
        if calcdir is None:
            self._calcdir = Path.cwd()
        else:
            self._calcdir = calcdir
        if path_exe is None:
            if self._cluster:
                self._path_exe = self._calcdir / "job.sh"
            else:
                if SPINAKER_EXE is None:
                    if self._logger is None:
                        print("Environment variable SPINAKER is not set. Either define this to the path of your"
                              " executable or provide the path via the path_exe input argument.")
                    else:
                        self._logger.warning("Environment variable SPINAKER is not set. Either define this to the"
                                             " path of your executable or provide the path via the path_exe input"
                                             " argument.")
                    self._path_exe = None
                else:
                    self._path_exe = Path(SPINAKER_EXE)
        else:
            self._path_exe = path_exe
        self._os = platform.system()
        self._process = None
        self._outputs = None
        self._errors = None
        self._i_ready = False

    @property
    def path_exe(self) -> Path:
        r"""
        :returns: The path to the executable.
        """
        return self._path_exe

    @path_exe.setter
    def path_exe(self, pe: Path) -> None:
        r"""
        Sets the path to the executable. Will be mostly used for assigning the name of the job-file.

        :param pe: path to the executable or jobfile
        """
        self._path_exe = pe

    @property
    def outputs(self) -> Union[None, str]:
        r"""
        :return: The information of the process that was written to stdout as a string. If process wasn't started
            yet this will be None.
        """
        if self._outputs is None:
            return None
        else:
            return self._outputs.strip()

    @property
    def errors(self) -> Union[None, str]:
        r"""
        :return: The errors of the process that was written to stderr as a string. If process wasn't started
            yet this will be None.
        """
        if self._errors is None:
            return None
        else:
            return self._errors.strip()

    @property
    def logger(self) -> Union[None, lg.Logger]:
        r"""
        :return: The logger of this class. If no logging instance is used fall back to printing to stdout.
        """
        return self._logger

    @logger.setter
    def logger(self, new_logger: lg.Logger) -> None:
        r"""
        Sets the logging instance

        :param new_logger: The new Logger.
        """
        self._logger = new_logger

    @property
    def process(self) -> Union[None, subprocess.Popen]:
        r"""
        :return: None if process has not been started, the instance of subprocess.Popen if started.
        """
        return self._process

    @property
    def i_ready(self) -> Union[None,bool]:
        r"""
        :return: Bool which states, if simulation came to an end. If extraction is not possible return None.
        """
        try:
            if self._i_ready == False:
                self.check_sim_end()
            return self._i_ready
        except Exception:
            return None

    @property
    def cluster(self) -> bool:
        r"""
        :return: If calculation is performed on cluster
        """
        return self._cluster

    @property
    def calcdir(self) -> Path:
        r"""
        :return: the calculation directory
        """
        return self._calcdir

    @calcdir.setter
    def calcdir(self, new_calcdir: Path) -> None:
        r"""
        Sets the calculation directory.

        :param new_calcdir: Path to new calc. dir.
        """
        self._calcdir = new_calcdir

    @property
    def executable(self) -> str:
        r"""
        :return: the path to the spinaker executable associated with this interface
        """
        return str(self._path_exe)

    @executable.setter
    def executable(self, path_exe: Path) -> None:
        r"""
        :param path_exe: sets the path of the executable
        """
        self._path_exe = path_exe

    def __call__(self) -> None:
        r"""
        This routine does different things depending on the value of cluster. If cluster is true a sbatch command
        followed by the path to the jobscript is performed by subprocess. This process is expected to run a very short
        time as it just sends the calculation order to a cluster node. If cluster is false a local process is created
        which runs a spinaker instance. This can take time. The python process does not wait for this to finish.
        """
        with change_directory(self._calcdir):
            if self.cluster:
                self._process = subprocess.Popen(args=["sbatch", self.executable], stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE, text=True)
            else:
                self._process = subprocess.Popen(args=[self.executable], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                 text=True)

    def wait_process_end(self, check_time_interval: float = 1, walltime: Union[None, float] = None) -> None:
        r"""
        This function waits until the process is completed. Be careful: This does not necessarily mean that the spinaker
        calculation is ready (e.g. if you are on the cluster and the process is just sending the calculation)
        or successfull.

        :param check_time_interval: This is the time interval for which the completion of the process is checked.
        :param walltime: If the summed of the performed time intervals exceeds the walltime the process is killed.
        :raise TimeoutError: If walltime is exceeded raise TimeoutError. Can be catched outside
        """
        if self._process is None:
            if self._logger is None:
                print("Process is None. Waiting makes no sense. Probably job was not started yet.")
            else:
                self._logger.error("Process is None. Waiting makes no sense. Probably job was not started yet.")
            return
        else:
            total_time = 0
            i_ready = False
            while not i_ready:
                try:
                    self._outputs, self._errors = self._process.communicate(input=None, timeout=check_time_interval)
                    if self._errors != "":
                        last_line = self.outputs.split(r"\n")[-1]
                        if self._logger is not None:
                            self._logger.error(f"Spinaker Error: {last_line}")
                        else:
                            print(f"Spinaker Error: {last_line}")
                    i_ready = True
                except subprocess.TimeoutExpired:
                    total_time += check_time_interval
                    if walltime is not None:
                        if total_time >= walltime:
                            self._process.kill()
                            self._outputs, self._errors = self._process.communicate()
                            raise TimeoutError
        if self._logger is not None:
            self._logger.debug(f"Outputs: {self.outputs}")
            self._logger.debug(f"Errors: {self.errors}")

    @property
    def job_id_cluster(self) -> Union[None, int]:
        r"""
        :return: This makes only sense for execution on the cluster. If a job is submitted an output like
            "Submitted batch job 8312693" is expected. This returns the job id of this job.
        """
        if not self.cluster:
            if self.logger is not None:
                self.logger.debug("Job-ID cluster None because execution was set to local.")
            return None
        try:
            return int(self.outputs.split("job ")[-1])
        except ValueError:
            if self.logger is not None:
                self.logger.error("Cannot convert cluster output to job-id. Maybe your cluster is communicating"
                                  " differently than the expected one.")
            return None

    def get_job_state(self) -> Union[None, str]:
        r"""
        :return: None if calculation is on local machine. If cluster is True a "sacct" request with the corresponding
            job-id collects the Job State (like PENDING, COMPLETING, COMPLETE, FAILED, etc...).
        """
        if not self.cluster:
            return None
        status_question_process = subprocess.Popen(args=["sacct", "-j", f"{self.job_id_cluster}",
                                                         "--format", "JobName,State"],
                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            outs, err = status_question_process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            if self.logger is not None:
                self.logger.error("Status request to cluster takes very long. Something might be wrong")
            else:
                print("Status request to cluster takes very long. Something might be wrong")
            return None
        out_array = outs.strip().split()
        if SPINAKER_EXE_NAME not in out_array:
            return PENDING
        else:
            spk_results = out_array[out_array.index(SPINAKER_EXE_NAME):]
            return spk_results[1]

    def check_sim_end(self) -> bool:
        r"""
        This routine checks if the simulation reached an end (No information if successfull or not). On a local machine
        this corresponds to the lifetime of the process. So, if the spinaker process ended also the simulation came to
        an end. On the cluster, this corresponds to the corresponding job (addressed by the job-id). It will be checked
        if the spinaker job is associated with Job State Code COMPLETED.

        :return: If the simulation came to an end (somehow)
        """
        if self.cluster:
            js = self.get_job_state()
            if js is None:
                self._i_ready = False
                return False
            else:
                if js == TIMEOUT:
                    self._errors="timeout"
                    if self.logger is None:
                        print(f"WARNING: Timout for job-id {self.job_id_cluster}")
                    else:
                        self.logger.warning(f"WARNING: Timout for job-id {self.job_id_cluster}")
                self._i_ready = True
                if js == FAILED:
                    self._errors="failed"
                    if self.logger is None:
                        print(f"WARNING: Failed for job-id {self.job_id_cluster}")
                    else:
                        self.logger.warning(f"WARNING: Failed for job-id {self.job_id_cluster}")
                if js == OUT_OF_MEMORY:
                    self._errors="outofmemory"
                    if self.logger is None:
                        print(f"WARNING: out of memory for job-id {self.job_id_cluster}")
                    else:
                        self.logger.warning(f"WARNING: out of memory for job-id {self.job_id_cluster}")
                return (js == COMPLETED) or (js == TIMEOUT) or (js == FAILED) or (js == OUT_OF_MEMORY)
        else:
            try:
                self._outputs, self._errors = self._process.communicate(input=None, timeout=0.1)
                self._i_ready = True
                return True
            except subprocess.TimeoutExpired:
                self._i_ready = False
                return False

    def wait_to_sim_end(self, walltime: Union[None, float] = None, check_time_interval: float = 5) -> None:
        r"""
        This routine waits for the simulation to end. In the case the calculation is performed on local machine this
        effectively does the same as the routine wait_process_end. For the computation of the cluster this really waits
        to the end of spinaker simulation opposed to wait_process_end which waits in this case just for completion of
        the batch sending process.

        :param walltime: Maximum allowed time to wait. If None this waits forever.
        :param check_time_interval: Time interval for checking frequency.
        :raise TimeoutError: if walltime is exceeded. Can be catched outside for error handling.
        """
        i_ready = False
        total_time = 0
        while not i_ready:
            if self.check_sim_end():
                break
            else:
                time.sleep(check_time_interval)
                total_time += check_time_interval
            if walltime is not None:
                if total_time >= walltime:
                    if self.logger is None:
                        print("Waiting for simulation end exceed walltime.")
                    raise TimeoutError
