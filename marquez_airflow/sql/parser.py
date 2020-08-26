# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlparse
from sqlparse.sql import T
from sqlparse.sql import TokenList

from . import SqlMeta


class SqlParser:
    """
    This class parses a SQL statement.
    """

    @staticmethod
    def parse(sql):
        """
        This method.
        """

        in_tables = []
        out_tables = []

        #
        statements = sqlparse.parse(sql)

        # We assume the SQL will contain only one statement
        tokens = TokenList(statements[0].tokens)

        #
        idx, token = tokens.token_next_by(t=T.Keyword)
        while token:
            if token.match(T.Keyword, values=['FROM', 'INNER JOIN']):
                idx, token = tokens.token_next(idx=idx)
                in_tables.append(next(token.flatten()).value)
            elif token.match(T.Keyword, values=['INTO']):
                idx, token = tokens.token_next(idx=idx)
                out_tables.append(next(token.flatten()).value)

            idx, token = tokens.token_next_by(t=T.Keyword, idx=idx)

        return SqlMeta(in_tables, out_tables)
