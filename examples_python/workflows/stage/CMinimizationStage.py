r"""
This module contains the stage-class for parallel execution of several minimizations.
"""
import pandas as pd
from spinterface.api.stages.IStage import IStage
from spinterface.api.CSpinakerExecution import CSpinakerExecution
from spinterface.api.CJobScriptSlurm import CJobScriptSlurm
from spinterface.api.simulations.CMinimization import CMinimization
import logging as lg
from typing import Union
from pathlib import Path


class CMinimizationStage(IStage):
    r"""
    Implemention of a stage. This stage is thought as a (parallel) execution helper for various independent minimization
    calculations. To register a minimization in this class define an instance of CMinimization and use the method
    add_minimization of this class. To start the stage you can call the instance of this stage (implemented in IStage).
    To wait for the stage to finish use the BaseClass wait_for_stage_end-method.
    """
    def __init__(self,label: str,define_core_api: bool = True, logger: Union[None,lg.Logger] = None,
                 exe: Union[None,CSpinakerExecution] = None, jobfile: Union[None, CJobScriptSlurm] = None,
                 simudir: Union[Path, None] = None) -> None:
        r"""
        Initializer for the minimization stage. This stage is thought to be useful when executing several independent
        minimization calculations at the same time while varying a single (or multiple parameters).

        :param define_core_api: Flag if logger, exe, jobfile and stage-simulation directory are defined during
            initialization of this class. If you use this class directly it is recommended to define all of them here
            and set this flag to True (Default). If the instance of this class is used within a workflow it is
            recommended to set this to False. The core API will then be defined with the settings of the workflow,
            respectively.
        :param label: Unique identifier for the instance of the stage
        :param logger: The information logger (See documentation (IStage))
        :param exe: Instance of CSpinakerExecution. Handles the interaction with the processes calling spinaker or job-
            scripts on a slurm cluster (See documentation (IStage)).
        :param jobfile: The instance of the jobfile-class. (See documentation (IStage)).
        :param simudir: The path to the stage simulation directory. If this is None the stage directory will be created
            under the name of the label in the current working directory.
        """
        super().__init__(label=label, logger=logger, exe=exe, jobfile=jobfile, simudir=simudir,
                         define_core_api=define_core_api)
        if self.logger is not None:
            self.logger.info(f"Minimization Stage {self.label} was created.")
        else:
            print(f"Minimization Stage {self.label} was created.")

    def add_minimization(self, minimization_simu: CMinimization, subdirname: str) -> None:
        r"""
        Adds a minimization simulation instance to the list of simulations. Overwrites the logger, the executable,
        the job-script to the one defined during initializing of the stage. Overwrites the simulations
        directory of the provided simulations.(in case define_core_api is True)

        :param minimization_simu: An Instance of CMinimization.
        :param subdirname: The name of the subdirectory for the simulations. Together with the path for the simulation
            directory provided initializing this class this overwrites the simulation directory set in the simulation
            before adding.
        """
        if self.logger is None:
            print(f"Add minimization with label: {minimization_simu.label}...")
        else:
            self._logger.info(f"Add minimization with label: {minimization_simu.label}...")
        self.add_simulation(simulation=minimization_simu,subdirname=subdirname)

