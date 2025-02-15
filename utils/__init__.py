from pathlib import Path
from typing import Any, Literal, Union, overload, NoReturn
import json
import sys

ROOT_DIR = Path(__file__).parent.parent


def write_json(filepath: Union[str, Path], data: Any, indent: int = 2) -> None:
    with open(filepath, "w") as f:
        json.dump(data, f, indent=indent)


def _load_json(filepath: Union[str, Path]) -> Union[dict, list]:
    with open(filepath, "r") as f:
        json_obj = json.load(f)
    return json_obj


@overload
def load_json_type_safe(
    filepath: Union[str, Path], return_type: Literal["dict"]
) -> dict:
    pass


@overload
def load_json_type_safe(
    filepath: Union[str, Path], return_type: Literal["list"]
) -> list:
    pass


def load_json_type_safe(
    filepath: Union[str, Path], return_type: Literal["dict", "list"]
) -> Union[dict, list]:
    """Handles the type checking for the expected return types.

    Parameters
    ----------
    filepath: str or Path
        The filepath to the JSON file to laod.
    return_type: Literal["dict", "list"]
        The expected return type.
    """
    loaded_json = _load_json(filepath)
    if return_type == "dict" and not isinstance(loaded_json, dict):
        raise ValueError(
            f"Expected type `dict` for file {filepath}, got type `{type(loaded_json)}`."
        )
    elif return_type == "list" and not isinstance(loaded_json, list):
        raise ValueError(
            f"Expected type `list` for file {filepath}, got type `{type(loaded_json)}`."
        )
    return loaded_json


def get_user_confirmation() -> None | NoReturn:
    while True:
        user_input = input("Continue? (y/n) ").strip().lower()
        if user_input == "y":
            return None
        elif user_input == "n":
            sys.exit(0)
        else:
            print("Please enter 'y' for yes or 'n' for no.")
