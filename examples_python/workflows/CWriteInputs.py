# -*- coding: utf-8 -*-
r"""
Input writing Helper Class.
"""
import json
from pathlib import Path
from typing import Any, Dict
from spinterface.constants import const_spinaker_filenames
from spinterface.constants import const_example_systems


class CWriteInputs:
    r"""
    Input writing helper class. Instances of this class are thought as mirrors to the different spinaker input files in
    ``.json``-format. This class can be used for all different spinaker input files and is kept quite simple.
    """

    def __init__(self, name: str, **kwargs) -> None:
        r"""
        Initializes the Input Class. You can either initialize the class by providing the content with all the key-
        value pairs in \**kwargs, or you can initialize from a given file or you can load a default. This default has
        to be stored within the example_input_files resource.

        :param name: Name of the input file. This should be a valid spinaker input file like algorithm.json (It is
            actually not tested if this is the case). Give the name together with the .json file ending.
        :param \**kwargs: Key-Value pairs of actual key-value pairs in the dictionary which is the content of the
            input file.
        """
        self._name = name
        self._content = dict(**kwargs)

    def __call__(self, where: Path = Path.cwd()) -> None:
        r"""
        Calls the writing of the input file.

        :param where: Where to write the file. This is meant to be path which shall contain the input file. A file will
            be created according to where/name (which includes the .json file ending)
        """
        with open(where / self._name, 'w') as f:
            json.dump(self._content, f, ensure_ascii=False, indent=4)
        if self._lattice_writer:
            self._lattice_writer()

    def load_default(self, name: str) -> None:
        r"""
        Loads a default content for a spinaker inputfile under the provided name.

        :param name: The name (including the `.json`-file ending) to the input file
        :raises ValueError: If the name is not an input-file listed in the example_input_files resource this error will
            be produced.
        """
        if name not in const_spinaker_filenames.INPUTFILENAMES:
            raise ValueError("Name of input files does not exist in the example_input_files resource.")
        else:
            self.initialize_from_file(inputfilepath=const_spinaker_filenames.INPUTFILES[name], set_name=True)

    def initialize_from_file(self, inputfilepath: Path, set_name: bool = True) -> None:
        r"""
        Initializes the input file with a given spinaker input json file.

        :param inputfilepath: The full path to the spinaker input file in json format (including the name)
        :param set_name: If the name of this instance (representing the actual kind of input file) shall be set
            according to the name of the file with which we initialize.
        """
        with open(inputfilepath) as f:
            self._content = json.load(f)
        if set_name:
            self._name = inputfilepath.name

    def insert_parameter(self, key: str, value: Any) -> None:
        r"""
        Inserts a parameter in the input file content

        :param key: Any Key (validity in terms of spinaker inputs is not tested)
        :param value: Any Value (validity in terms of spinaker inputs is not tested)
        """
        if key in self._content.keys():
            raise ValueError("Parameter key already present")
        else:
            self._content[key] = value

    def overwrite_parameter(self, key: str, value: Any) -> None:
        r"""
        Adds a key-value pair to the content. Deviates from the methods ```insert_parameter``` and
        ```adjust_parameter``` by the behaviour if the key is not existing. In this keys the requested key will be
        created. If it is there the corresponding value will be overwritten.
        """
        self._content[key] = value

    def adjust_parameter(self, key: str, value: Any) -> None:
        r"""
        Adjusts a parameter in the input file

        :param key: Any Key (validity in terms of spinaker inputs is not tested)
        :param value: Any Value (validity in terms of spinaker inputs is not tested)
        """
        if self._content.get(key) is not None:
            self._content[key] = value
        else:
            raise KeyError("Not a valid input key.")

    def get_parameter(self, key: str) -> Any:
        r"""
        Gets the requested parameter by its key

        :param key: Any Key (validity in terms of spinaker inputs is not tested)
        :return: The value
        """
        try:
            return self._content[key]
        except KeyError:
            raise KeyError("Parameter not present in input file")

    @property
    def name(self) -> str:
        r"""
        return: the name of the input file (including the .json-ending)
        """
        return self._name

    @property
    def content(self) -> Dict[Any, Any]:
        r"""
        return: the content of the input file as dictionary
        """
        return self._content
