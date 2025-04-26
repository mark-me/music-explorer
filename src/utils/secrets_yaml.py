import yaml
import os


class SecretsYAML:
    """Manages secrets stored in a YAML file.

    This class provides methods for reading, writing, and validating secrets
    stored in a YAML file, specific to a given application.
    """
    def __init__(self, file_path: str, app: str, expected_keys: set) -> None:
        """Initializes SecretsYAML with file path, app name, and expected keys.

        Args:
            file_path (str): The path to the YAML file.
            app (str): The name of the application.
            expected_keys (set): A set of expected keys for the application's secrets.
        """
        self._file = file_path
        self.__create_path()
        self._app = app
        self._expected_keys = expected_keys

    def __create_path(self) -> None:
        """Creates the directory path for the secrets file if it doesn't exist."""
        path = os.path.dirname(self._file)
        isExist = os.path.isdir(path)
        if not isExist:  # Create a new directory because it does not exist
            os.makedirs(path)

    def is_complete(self) -> tuple[bool, str]:
        """Checks if the secrets file is complete.

        This method checks if the secrets file exists, if it contains settings for the specified app,
        and if all expected keys are present within the app's settings.

        Returns:
            tuple[bool, str]: A tuple containing a boolean indicating whether the file is complete
                and a string describing the validation result.
        """
        # Check file existence
        if not os.path.isfile(self._file):
            return False, f"There is no config file for secrets in {self._file}"
        # Load the YAML file
        with open(self._file, "r") as file:
            try:
                yaml_data = yaml.safe_load(file)
            except yaml.YAMLError as e:
                return {"status_code": 500, "detail": f"Error loading file: {str(e)}"}

        # Check if there are any app settings
        if self._app not in yaml_data.keys():
            return False, f"No settings for {self._app} in : {self._file}"

        # Check if there are apps
        missing_keys = self._expected_keys - set(yaml_data[self._app].keys())
        if missing_keys:
            return (
                False,
                f"Missing keys {', '.join(missing_keys)} for {self._app} in file {self._file}",
            )

        return True, "YAML file is valid"

    def write_secrets(self, dict_secrets: dict) -> None:
        """Writes secrets for an app to the YAML file.

        This method writes or updates the secrets for the specified application in the YAML file.
        If the file doesn't exist, it creates a new one. If the app's section doesn't exist,
        it creates it.

        Args:
            dict_secrets (dict): A dictionary containing the secrets to write.
        """
        try:
            with open(self._file, "r") as file:
                yaml_data = yaml.safe_load(file)
        except IOError:
            yaml_data = {}
            with open(self._file, "w") as file:
                pass
        yaml_data[self._app] = dict_secrets
        with open(self._file, "w") as file:
            yaml.safe_dump(yaml_data, file)

    def read_secrets(self) -> dict:
        """Reads the secrets for the app from the YAML file.

        This method reads and returns the secrets associated with the specified application.
        It first checks if the secrets file is complete. If not, it returns None.

        Returns:
            dict: A dictionary containing the app's secrets, or None if the file is incomplete.
        """
        is_complete, value = self.is_complete()
        if is_complete:
            with open(self._file, "r") as file:
                yaml_data = yaml.safe_load(file)
            return yaml_data[self._app]
        else:
            return None
