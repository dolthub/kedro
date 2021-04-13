# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""``DoltDataSet`` loads/saves data from/to a CSV file using an underlying
filesystem (e.g.: local, S3, GCS). It uses pandas to handle the CSV file.
"""
from copy import deepcopy
import datetime
from pathlib import PurePosixPath
import tempfile
from typing import Any, Dict, Optional, Literal, List

import doltcli as dolt
import fsspec
import pandas as pd

from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


def load_database_config(db_config, br_config):
    db = dolt.Dolt(db_config["filepath"])

    if not db.status().is_clean:
        raise ValueError("DoltDB status must be clean")

    if br_config is None:
        pass
    elif "commit" in br_config and br_config["commit"] is not None:
        db.checkout(br_config["commit"])
    elif "branch" in br_config and br_config["branch"] is not None:
        db.checkout(br_config["commit"], checkout_branch=False)

    return db


class DoltDataSet(AbstractVersionedDataSet):
    """``DoltDataSet`` loads/saves data from/to a CSV file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses pandas to handle the CSV file.

    Example:
    ::

        >>> from kedro.extras.datasets.pandas import DoltDataSet
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({'col1': [1, 2], 'col2': [4, 5],
        >>>                      'col3': [5, 6]})
        >>>
        >>> # data_set = DoltDataSet(filepath="gcs://bucket/test.csv")
        >>> data_set = DoltDataSet(filepath="test.csv")
        >>> data_set.save(data)
        >>> reloaded = data_set.load()
        >>> assert data.equals(reloaded)

    """

    """
    TableConfig :
      - tablename
      - indexes

    BranchConfig :
      - new branch, merge back to target
      - new branch, don't merge
      - add directly to branch
      - default: add directly to master

    RemoteConfig :
      - where to push new commits
      - default: None

    Metadata:
      - write to separate Dolt database, specify config
      - can use same DB for demo, but generally bad idea (have to use the same branchconfig)
      - 3rd party metadata store
      - default: None
    """

    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {"index": False}  # type: Dict[str, Any]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        tablename: str,
        index: Optional[List[str]] = None,
        checkout_config: Optional[str] = None,
        meta_config: Optional[Dict[str, Any]] = None,
        meta_checkout_config: Optional[Dict[str, Any]] = None,
        remote_config: Optional[Dict[str, Any]] = None,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        _fs_open_args_save.setdefault("mode", "w")
        _fs_open_args_save.setdefault("newline", "")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

        self._tablename = tablename
        self._index = index
        self._doltdb = load_database_config(dict(filepath=filepath), checkout_config)
        self._checkout_config = checkout_config or dict(branch="master")

        self._metadb = None
        self._meta_checkout_config = meta_checkout_config
        if meta_config is not None:
            self._metadb = load_database_config(meta_config, meta_checkout_config)
            self._meta_tablename = meta_config["tablename"]

        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        self._doltdb = dolt.Dolt(load_path)

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            load_args=self._load_args,
            save_args=self._save_args,
            tablename=self._tablename,
        )

    def _meta(self, kind: Literal["load", "save"], commit: str, tablename: str):
        """
        create table if it doesn't exist
        record the change that happened
        """
        if self._meta_tablename is None:
            return
        meta = self._meta_tablename

        timestamp = int(datetime.datetime.now().timestamp())
        session_id = ...

        tables = self._metadb.sql(
            f"select * from information_schema.tables where table_name = '{meta}'",
            result_format="json"
        )

        if len(tables["rows"]) < 1:
            create_table = f"""
                create table {meta} (
                    kind text,
                    commit text,
                    tablename text,
                    timestamp bigint,
                    session_id text,
                    primary key (kind, commit, tablename, timestamp, session_id)
                )
            """
            self._metadb.sql(create_table)

        self._metadb.sql(
            f"""
                insert into
                {meta} (kind, commit, tablename, timestamp, session_id)
                values ('{kind}', '{commit}', '{tablename}', {timestamp}, '{session_id}')
                """,
            result_format="csv",
        )

    def _commit(self):
        try:
            self._doltdb.sql(f"select dolt_commit('-am', 'Automated commit')", result_format="json")
        except dolt.DoltException:
            pass

    def _load(self, version: Optional[str] = None) -> pd.DataFrame:
        """
        fetch data
        record audit
        commit the read metadata
        """
        commit = version or self._doltdb.head
        sql = dolt.utils.get_read_table_asof_query(self._tablename, commit)
        df = dolt.utils.read_table_sql(self._doltdb, sql, result_parser=lambda x: pd.read_csv(x, **self._load_args))
        self._meta(kind="load", commit=commit, tablename=self._tablename)
        self._commit()
        if self._index is not None:
            return df.set_index(self._index).sort_index()
        return df

    def _save(
        self,
        data: pd.DataFrame,
        mode: Literal["replace", "update"] = "replace",
    ) -> None:
        """
        navigate to appropriate branch
        load the table
        record the metadata
        commit the new table and metadata
        """
        if self._index is not None:
            data = data.reset_index()

        def writer(filepath: str):
            clean = data.dropna(subset=self._index)
            clean.to_csv(filepath, **self._save_args)

        with tempfile.NamedTemporaryFile() as f:
            dolt.utils._import_helper(
                dolt=self._doltdb,
                table=self._tablename,
                write_import_file=writer,
                primary_key=self._index,
                import_mode=mode,
            )

        self._commit()
        self._meta(kind="save", commit=self._doltdb.head, tablename=self._tablename)
        self._commit()

        self._invalidate_cache()

    def _exists(self) -> bool:
        tables = self._metadb.sql(
            f"select * from information_schema.tables where table_name = '{self._tablename}'",
            result_format="json"
        )
        return len(tables["rows"]) > 1

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
