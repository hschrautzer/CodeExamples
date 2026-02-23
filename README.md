# CodeExamples
The atomistic spin simulation code named "Spinaker" is not yet publically available as we are currently publishing it.
This repository contains code examples of some of my contributions to this project. Generally, the project is separated
into two parts:
- Spinaker: represents the engine of the code and is programmed in Fortran. It performs e.g. optimization tasks or Monte
Carlo simulations.
- Spinterface: represents various pre- and postprocessing steps allowing to analyze the results of Spinaker.

## Workflow part of spinterface (Python)
To reflect my Python coding I included the workflow part that I implemented for Spinaker.
New physical problems often require the user to create workflows combining several of Spinakers' algorithms sequentially
to perform a certain task. However, if a workflow is established is should also be available to other users.
Therefore, I created this workflow part of Spinterface. Some Python example files are included in this repository and
the idea is sketched below:

### Objective
The objective was to create a blueprint for Python workflows:

- Provide a blueprint structure that allows the proficient user to orchestrate workflows.
- These workflows can contain any parallel or sequential combination of Spinaker's algorithms alongside with further
pre- post- and intermediate analyzing steps.
- The execution part is automatized and able to run on a HPC cluster or on a local machine.

### Structure
The above objectives are realized by the chosing the following nested structure:
1. Simulation: Corresponds to a certain Spinaker algorithm task or an analyzing step.
2. Stage: Corresponds to several Simulations that are executed in parallel.
3. Workflow: Corresponds to a sequential combination of Stages.

### Implementation
For each of the above levels an interface (abstract base class) is provided: `ISimulation`, `IStage`, `IWorkFlow`.
Concrete implementations are then realized by inheriting from these interfaces. E.g. the Spinaker minimization algorithm
corresponds to `CMinimization` inheriting from `ISimulation`. Similarly, `CMinimizationStage` corresponds to several
minimization tasks to be executed in parallel. The topmost structure is then a workflow that realizes the desired task
and inherits from `IWorkFlow`.

Three classes realize the execution of Spinaker (`CSpinakerExecution`), organizing inputs for Spinaker (`CWriteInputs`)
and defining HPC cluster job-settings (`CJobScriptSlurm`).
