from typing import Optional
from requests import Response
import logging

from .data_types import APIHandler
from utils.logging import LoggedClass, log_once
from utils.data_types import AssessedBiomarkerEntity, Synonym


class UniprotHandler(APIHandler, LoggedClass):
    """Handles Uniprot API responses and data processing."""

    def __call__(self, response: Response) -> Optional[AssessedBiomarkerEntity]:
        """Process Uniprot API response.

        Parameters
        ----------
        response: Response
            Response from Uniprot API call

        Returns
        -------
        Optional[dict[str, list[str]]]
            Dictionary with recommended name and synonyms or None if processing fails
        """
        try:
            uniprot_data = response.json()["protein"]
            self.debug(
                f"Processing Uniprot data: {uniprot_data.get('recommendedName', {})}"
            )

            synonyms = []

            # Get recommended short names
            recommended_short_names = uniprot_data.get("recommendedName", {}).get(
                "shortName", []
            )
            for short_name in recommended_short_names:
                name = short_name.get("value")
                if name:
                    synonyms.append(name)
                    self.debug(f"Added recommended short name: {name}")

            # Get alternative names
            for alt_name in uniprot_data.get("alternativeName", []):
                # Get full alternative name
                full_name = alt_name.get("fullName", {}).get("value")
                if full_name:
                    synonyms.append(full_name)
                    self.debug(f"Added alternative full name: {full_name}")

                # Get alternative short names
                for short_name in alt_name.get("shortName", []):
                    name = short_name.get("value")
                    if name:
                        synonyms.append(name)
                        self.debug(f"Added alternative short name: {name}")

            # Get recommended full name
            recommended_name = (
                uniprot_data.get("recommendedName", {}).get("fullName", {}).get("value")
            )
            if not recommended_name:
                self.warning("No recommended name found in Uniprot response")
                return None

            self.debug(f"Successfully processed Uniprot data for {recommended_name}")
            return AssessedBiomarkerEntity(
                recommended_name=recommended_name,
                synonyms=[Synonym(synonym=s) for s in synonyms],
            )

        except KeyError as e:
            log_once(
                self.logger,
                f"Missing required field in Uniprot response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None
        except Exception as e:
            log_once(
                self.logger,
                f"Error processing Uniprot response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None


# Create singleton instance
uniprot_handler = UniprotHandler()
