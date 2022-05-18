# Standard library imports
import yaml

# Third party library imports
from pyaml_env import parse_config

# Local library imports


class ConfigLoader():
    """ A class that loads a specific configuration file. 
    
        Any instance of this class provides a public get_item 
        function that retrieves values of a specific 
        configuration item.
    """

    def __init__(self, config_file, search_number=None):
        self.config = self._load_configuration(config_file, search_number)
    
    def _load_configuration(self, config_file, search_number):
        """ Loads a specific YAML file for either a specific search query
            or the emailer parameters. 
            
            Args: 
                config_file: The path to the config file
                search_number: The integer index of the search query
                be completed. Not applicable to emailer config.
            
            Returns:
                A dictionary of parameters and values. 
                
            Raises:
                A config not found error.
        """

        config = parse_config(config_file)

        if not config:
            raise FileNotFoundError("Failed to open file: " + config_file)

        if search_number is not None:
            config = list(config.values())[search_number]
        
        return config

    def get_item(self, config_item):
        """ Get the value of an item from the config file.
            Values can be separated by commas.

            Args:
                config_item: Name of the item to look up.

            Returns:
                A string for the requested item.

        """

        config_item = str(self.config.get(config_item))

        return config_item
