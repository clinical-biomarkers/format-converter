from typing import Optional
import logging
from pymed import PubMed
import os
from time import sleep
from xml.etree.ElementTree import ParseError

from utils.logging import LoggedClass, log_once
from utils.data_types import Citation
from .data_types import LibraryHandler


class PubmedHandler(LibraryHandler, LoggedClass):
    """Handles Pubmed API responses and data processing."""

    def __call__(
        self,
        id: str,
        max_retries: int = 3,
        _: int = 5,
        sleep_time: int = 1,
    ) -> tuple[int, Optional[Citation]]:
        """Processes Pubmed API response.

        Parameters
        ----------
        id: str
            Pubmed ID for the paper to lookup

        Returns
        -------
        tuple[int, Optional[Citation]]
            Tuple containing the amount of API calls attempted and the citation
            information (or None on failure)
        """
        email = os.getenv("EMAIL")
        if email is None:
            log_once(
                self.logger,
                "Failed to find EMAIL environment variable. Check .env file. Skipping PubMed API calls...",
                logging.ERROR,
            )
            return 0, None

        pubmed_api_key = os.getenv("PUBMED_API_KEY")
        if pubmed_api_key is None:
            log_once(
                self.logger,
                "Failed to find PUBMED_API_KEY environment variable. Check .env file. PubMed API calls will likely rate limit",
                logging.WARNING,
            )

        pubmed = PubMed(tool="CFDE BiomarkerKB", email=email)
        if pubmed_api_key:
            pubmed.parameters.update({"api_key": pubmed_api_key})

        query = f"PMID: {id}"

        attempt = 0
        while attempt < max_retries:
            try:
                articles = pubmed.query(query)
                article = next(articles)
                title = article.title
                journal = article.journal
                authors = ", ".join(
                    [
                        f"{author['lastname']} {author['initials']}"
                        for author in article.authors
                    ]
                )
                publication_date = str(article.publication_date)

                citation = Citation(
                    title=title,
                    journal=journal,
                    authors=authors,
                    date=publication_date,
                    reference=[],
                    evidence=[],
                )
                return attempt + 1, citation
            except StopIteration as e:
                self.warning(f"Error: No articles found for Pubmed ID: {id}\n{e}")
                return attempt + 1, None
            except ParseError as e:
                self.exception(
                    f"Failed to parse (attempt {attempt}/{max_retries}) Pubmed data for Pubmed ID: {id}\n{e}"
                )
            except Exception as e:
                self.exception(
                    f"Unexpected error while fetching (attempt {attempt}/{max_retries}) Pubmed data for Pubmed ID: {id}\n{e}"
                )

            attempt += 1
            self.debug(f"Sleeping for {sleep_time} seconds...")
            sleep(sleep_time)

        log_once(
            self.logger,
            f"Failed to complete API call for Pubmed, ID: {id} after {max_retries} attempts",
            logging.ERROR,
        )
        return attempt + 1, None


# Create singleton instance
pubmed_handler = PubmedHandler()
