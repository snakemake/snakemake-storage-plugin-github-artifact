import os
from pathlib import Path
from typing import Dict
from datetime import datetime
import zipfile
from urllib.parse import urlparse
from typing import Any, Iterable, Optional, List
import subprocess as sp

import requests

from snakemake_interface_storage_plugins.storage_provider import (  # noqa
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface


# Raise errors that will not be handled within this plugin but thrown upwards to
# Snakemake and the user as WorkflowError.
from snakemake_interface_common.exceptions import WorkflowError  # noqa


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
# Inside of the provider, you can use self.logger (a normal Python logger of type
# logging.Logger) to log any additional informations or
# warnings.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.token = os.getenv("GITHUB_TOKEN")
        self.runtime_token = os.getenv("ACTIONS_RUNTIME_TOKEN")
        self.runtime_url = os.getenv("ACTIONS_RUNTIME_URL")
        self.run_id = os.getenv("GITHUB_RUN_ID")
        self.repo = os.getenv("GITHUB_REPOSITORY")

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="gh://results/data.txt",
                type=QueryType.ANY,
                description="A file or directory stored as a github actions artifact",
            )
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return None

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 0.25

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme == "gh" and parsed.netloc and parsed.path:
            return StorageQueryValidationResult(valid=True, query=query)
        else:
            return StorageQueryValidationResult(
                valid=False,
                query=query,
                reason="Query does not start with gh:// or does not "
                "contain a path to a file or directory.",
            )


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
# Inside of the object, you can use self.provider to access the provider (e.g. for )
# self.provider.logger, see above, or self.provider.settings).
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        parsed_query = urlparse(self.query)
        self.path = parsed_query.netloc + parsed_query.path
        self.artifact_name = str(self.path)#.replace("/", "_")
        self.path = Path(self.path)
        self._cache = None

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        return str(self.path)

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    @property
    def _artifacts(self) -> Dict[str, Any]:
        res = requests.get(
            f"https://api.github.com/repos/{self.provider.repo}/actions/artifacts",
            headers=self._headers(accept="application/vnd.github+json"),
        )
        res.raise_for_status()
        return res.json()

    @property
    def _artifact(self) -> Optional[Dict[str, Any]]:
        if self._cache is None:
            matching = sorted(
                (
                    artifact
                    for artifact in self._artifacts["artifacts"]
                    if artifact["name"] == self.artifact_name
                ),
                key=lambda artifact: datetime.fromisoformat(artifact["updated_at"]),
                reverse=True,
            )
            if matching:
                self._cache = matching[0]
        return self._cache

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        return self._artifact is not None

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        return datetime.fromisoformat(self._artifact["updated_at"]).timestamp()

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        return self._artifact["size_in_bytes"]

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        # Optionally, this can make use of the attribute self.is_ondemand_eligible,
        # which indicates that the object could be retrieved on demand,
        # e.g. by only symlinking or mounting it from whatever network storage this
        # plugin provides. For example, objects with self.is_ondemand_eligible == True
        # could mount the object via fuse instead of downloading it.
        # The job can then transparently access only the parts that matter to it
        # without having to wait for the full download.
        # On demand eligibility is calculated via Snakemake's access pattern annotation.
        # If no access pattern is annotated by the workflow developers,
        # self.is_ondemand_eligible is by default set to False.
        download_url = self._artifact()["archive_download_url"]
        res = requests.get(
            download_url, headers=self._headers(accept="application/vnd.github+json")
        )
        res.raise_for_status()
        redirect_url = res.header()["Location"]
        res = requests.get(
            redirect_url, headers=self._headers(accept="application/zip"), stream=True
        )
        # extract file from zip and store under self.local_path()
        with zipfile.ZipFile(res.content) as zip_file:
            zip_file.extractall(self.local_path(), [self.local_path().name])

    def _headers(
        self,
        accept: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> Dict[str, str]:
        accept = {"Accept": accept} if accept else {}
        content_type = {"Content-Type": content_type} if content_type else {}
        return (
            {
                "Authorization": f"Bearer {self.provider.token}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
            | accept
            | content_type
        )

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        sp.run(
            [
                "node",
                str(Path(__file__).parent / "store_object.js"),
                str(self.local_path()),
                str(self.local_path().parent),
                self.artifact_name,
            ],
            check=True,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
        )
        self._cache = None

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        pass

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        return []
