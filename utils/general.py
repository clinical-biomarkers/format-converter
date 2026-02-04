import sys
from typing import NoReturn

def get_user_confirmation() -> None | NoReturn:
    """Prompts the user for a confirmation or denial."""
    while True:
        user_input = input("Continue? (y/n) ").strip().lower()
        if user_input == "y":
            return None
        elif user_input == "n":
            print("Conversion cancelled by user")
            sys.exit(0)
        else:
            print("Please enter 'y' for yes or 'n' for no.")

def confirmation_message_complete() -> None:
    print("Confirmation prompts over.")
    print("Safe to background process now (Ctrl+Z, then 'bg' and 'disown').")
