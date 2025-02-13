from typing import Any, Optional
import logging
from pymed import PubMed
import os
from time import sleep

from utils.logging import LoggedClass, log_once
from utils.data_types import (
    LibraryHandler,
    Citation,
    CacheableDataModelObject,
    RateLimiter,
)


class PubmedHandler(LibraryHandler, LoggedClass):
    """Handles Pubmed API responses and data processing."""

    def _extract_article_data(self, article: Any) -> Optional[Citation]:
        try:
            title = article.title
            authors = ", ".join(
                [
                    f"{author['lastname']} {author['initials']}"
                    for author in article.authors
                ]
            )
            publication_date = str(article.publication_date)

            # Handle journal vs book
            if hasattr(article, "journal"):
                journal = article.journal
            else:
                journal = getattr(article, "book_title", "Book")

            return Citation(
                title=title,
                journal=journal,
                authors=authors,
                date=publication_date,
                reference=[],
                evidence=[],
            )
        except AttributeError as e:
            self.error(f"Missing required attribute while processing article: {e}")
            return None
        except Exception as e:
            self.error(f"Unexpected error processing article: {e}")
            return None

    def __call__(
        self,
        id: str,
        resource: str,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ) -> tuple[int, Optional[CacheableDataModelObject]]:
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
                self._check_limit(resource=resource, rate_limiter=rate_limiter)
                articles = pubmed.query(query)
                article = next(articles)
                self._record_call(resource=resource, rate_limiter=rate_limiter)

                citation = self._extract_article_data(article)
                if citation:
                    return attempt + 1, citation
                return attempt + 1, None

            except StopIteration as e:
                self._record_call(resource=resource, rate_limiter=rate_limiter)
                self.warning(f"Error: No articles found for Pubmed ID: {id}\n{e}")
                return attempt + 1, None
            except Exception as e:
                self._record_call(resource=resource, rate_limiter=rate_limiter)
                attempt += 1
                self.exception(
                    f"Unexpected error while fetching (attempt {attempt}/{max_retries}) Pubmed data for Pubmed ID: {id}\n{e}"
                )
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
