r"""
This module contains the spinaker api simulation class CMinimization.
"""
import pandas as pd

from spinterface.api.simulations.ISimulation import ISimulation
import logging as lg
from spinterface.api.CSpinakerExecution import CSpinakerExecution
from spinterface.api.CJobScriptSlurm import CJobScriptSlurm
from spinterface.api.CWriteInputs import CWriteInputs
from spinterface.core.lattice.CSpinLattice import CSpinLattice
from pathlib import Path
from typing import Union, Dict, Any


class CMinimization(ISimulation):
    r"""
    Implementation of the ISimulation class for Minimization simulations with Spinaker.
    """
    SPIN_INI_FNAME = "spin_i.dat"
    SPIN_FIN_FNAME = "spin_min_end.dat"
    INFO_MIN_FILE = "info_min.dat"

    def __init__(self, label: str, lattice: CWriteInputs, interaction: CWriteInputs, define_core_api: bool = True,
                 minimization: Union[CWriteInputs,None] = None, algorithm: Union[CWriteInputs,None] = None,
                 general: Union[CWriteInputs,None] = None, spinlattice_ini: Union[CSpinLattice, None]= None,
                 simudir: Union[None,Path] = None, logger: Union[lg.Logger,None] = None,
                 exe: Union[None,CSpinakerExecution] = None, jobfile: Union[None, CJobScriptSlurm] = None) -> None:
        r"""
        Initializes the Minimization Simulation instance.

        :param label: Unique identifier for the simulation
        :param lattice: CWriteInputs Instance of lattice.json
        :param interaction: CWriteInputs Instance of interaction.json
        :param define_core_api: Flag if logger, exe, jobfile and simulation directory are defined during initialization
            of this class. If you use an implementation of this class directly it is recommended to define all of them
            here and set this flag to True (Default). If the instance of this class is used within a workflow or a stage
            it is recommended to set this to False. The core API will then be defined with the settings of the stage or
            workflow, respectively.
        :param minimization: CWriteInputs Instance of minimization.json. If None the default input based on
            example input folders will be created.
        :param algorithm: CWriteInputs Instance of algorithm.json. If None the default input based on example input
            folder will be created. It will be made sure in any case that the flag: i_minimization is true.
        :param general: CWriteInputs instance of general.json. If None the default input based on example input folders
            will be created.
        :param spinlattice_ini: An instance of CSpinLattice. If None a default random lattice will be created based on
            the lattice.json input file.
        :param simudir: Directory in which the minimization shall be performed. Will only be considered if
            define_core_api is True. (See ISimulation Initialization Documentation)
        :param logger: Information logger. Will only be considered if
            define_core_api is True. (See ISimulation Initialization Documentation)
        :param exe: Executable. Will only be considered if
            define_core_api is True. (See ISimulation Initialization Documentation)
        :param jobfile: Instance of CJobScriptSlurm. Will only be considered if
            define_core_api is True. (See ISimulation Initialization Documentation)
        :raise KeyError: If algorithm input file does not contain the keyword i_minimization (For Developers: See
        comment in source code)
        """

        inputfiles = self._parse_inputs(lattice=lattice,interaction=interaction,minimization=minimization,
                                        algorithm=algorithm,general=general)
        self._spinlattice_ini = spinlattice_ini
        super().__init__(label=label,define_core_api=define_core_api,simudir=simudir,logger=logger,exe=exe,
                         jobfile=jobfile,**inputfiles)

    @property
    def spinlattice_ini(self) -> Union[None,CSpinLattice]:
        r"""
        :return: The instance of CSpinLattice describing the initial state of the minimziation.
        """
        return self._spinlattice_ini

    def _parse_inputs(self,lattice: CWriteInputs, interaction: CWriteInputs,
                      minimization: Union[CWriteInputs,None] = None, algorithm: Union[CWriteInputs,None] = None,
                      general: Union[CWriteInputs,None] = None) -> Dict[str,CWriteInputs]:
        r"""
        Parses the inputs. See documentation of __init__ method.
        """
        inputdict = {"lattice": lattice, "interaction": interaction}
        if general is None:
            l_general = CWriteInputs(name="general.json")
            l_general.load_default("general.json")
        else:
            l_general = general
        inputdict["general"] = l_general
        if algorithm is None:
            # @DEVELOPERS: If (for some really not recommended reason) you want to change the keyword name in spinakers
            # input file from "i_minimization" to something else, you have to modify it here as well.
            l_algorithm = CWriteInputs(name="algorithm.json", i_minimization=True)
        else:
            l_algorithm = algorithm
        try:
            _ = l_algorithm.get_parameter(key="i_minimization")
        except KeyError:
            raise KeyError("The algorithm.json input file does not contain the keyword i_minimization.")
        inputdict["algorithm"] = l_algorithm
        if minimization is None:
            l_minimization = CWriteInputs(name="minimization.json")
            l_minimization.load_default("minimization.json")
        else:
            l_minimization = minimization
        try:
            l_minimization.adjust_parameter(key="spin_ini_file",value=self.SPIN_INI_FNAME)
        except KeyError:
            raise KeyError("The minimization.json input file does not contain the keyword spin_ini_file.")
        inputdict["minimization"] = l_minimization
        return inputdict

    def write_simulation_inputs(self) -> None:
        r"""
        Writes the simulation inputs to the simulation directory. If the spinlattice_ini is None a random state based on
        the lattice.json input file will be created.
        """
        for inputfile in self.inputfiles.values():
            inputfile(where=self.simudir)
        if self._spinlattice_ini is None:
            self._spinlattice_ini = CSpinLattice(path_lattice_input=self.simudir / "lattice.json")
            self._spinlattice_ini.add_random_state()
        self._spinlattice_ini(outpath=self.simudir / self.SPIN_INI_FNAME)

    def __call__(self, walltime: Union[float,None] = None, check_time_interval: float = 1,
                 cluster_block_python: bool = False, local_block_python: bool = True) -> None:
        r"""
        Starts the simulation. If calculation is performed on local machine the python process will be blocked until the
        end of the Spinaker process (unless the variable local_block_python is False).
        This blocks the server-client parallelism on the local machine to avoid race -conditions.
        If calculation is performed on the cluster the default setting is that many processes (in terms of
        sending batch-scripts) can be spawned without blocking the python process. You can force the blocking behaviour
        on the cluster by the flag: cluster_block_python.

        :param cluster_block_python: The Default to this is False. If active the python process is blocked until the end
            of the simulation on the cluster.
        :param local_block_python: The Default to this is True. If not active the python process is not blocked and
            multiple spinaker simulations can be spawned. Be careful: if this is False you probably have to wait
            elsewhere. Since if the python process ends before spinaker came to an end the child process (spinaker) will
            also be killed. But this might be useful if incorporating this in a workflow.
        :param walltime: Walltime used for local machine or used on cluster if cluster_block_python is active.
        :param check_time_interval: Time intervall used for checking end of simulation on local machine or on cluster if
            cluster_block_python is active.
        :raise TimoutError: If sending of batch script on the cluster exceeds 10s a TimeoutError will be raised. Can
            be catched outside for error handling of this specific simulation. Also a TimeoutError will be raised if
            waiting for simulation end exceeds the walltime (this is also true if cluster_block_python is active)
        """
        self.logger.info("Write inputs for minimization...")
        self.write_simulation_inputs()
        self.logger.info("...done")
        super().__call__(walltime=walltime,check_time_interval=check_time_interval,
                         cluster_block_python=cluster_block_python,local_block_python=local_block_python)

    @property
    def spinlattice_fin(self) -> Union[None,CSpinLattice]:
        r"""
        :return: None if final spin file is not found. Else the spin lattice instance.
        """
        try:
            return CSpinLattice(path_spinfile=self.simudir / self.SPIN_FIN_FNAME)
        except FileNotFoundError:
            return None

    def get_information(self) -> Union[None,Dict[str,Any]]:
        r"""
        :return: If spinaker is not yet ready return None. Otherwise, add the minimization specific information
        """
        if not self.spinaker_completed:
            self.logger.warning(f"Spinaker is not yet ready. Can't extract information for simu. {self.label}")
            return None
        info_dict = self.get_meta_information()
        try:
            df = pd.read_csv(self.simudir / self.INFO_MIN_FILE,sep=r"\s+",index_col=False)
        except FileNotFoundError:
            self.logger.error(f"Can't extract information for simu. {self.label}. "
                              f"File: {self.INFO_MIN_FILE} was not found.")
            return info_dict
        try:
            first_row = df.iloc[0]
        except IndexError:
            self.logger.error(f"First row parsing didn't work. Probably {self.INFO_MIN_FILE} is empty.")
            return info_dict
        last_row = df.iloc[-1]
        info_dict["iterations"] = int(last_row["iteration"])
        info_dict["energy_initial"] = float(first_row["energy"])
        info_dict["energy_per_spin_initial"] = float(first_row["energy_per_spin"])
        info_dict["energy_final"] = float(last_row["energy"])
        info_dict["energy_per_spin_final"] = float(last_row["energy_per_spin"])
        info_dict["energy_monotonic_decrease"] = bool(df["energy"].is_monotonic_decreasing)
        info_dict["torque_initial"] = float(first_row["max_torque"])
        info_dict["torque_final"] = float(last_row["max_torque"])
        info_dict["torque_max"] = float(df["max_torque"].max())
        info_dict["forcenorm_initial"] = float(first_row["norm_force"])
        info_dict["forcenorm_final"] = float(last_row["norm_force"])
        info_dict["forcenorm_max"] = float(df["norm_force"].max())
        info_dict["torquenorm_initial"] = float(first_row["norm_torque"])
        info_dict["torquenorm_final"] = float(last_row["norm_torque"])
        info_dict["torquenorm_max"] = float(df["norm_torque"].max())
        return info_dict