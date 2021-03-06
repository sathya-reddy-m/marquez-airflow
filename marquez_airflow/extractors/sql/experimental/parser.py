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

import logging

import sqlparse
from sqlparse.sql import T
from sqlparse.sql import TokenList

from marquez_airflow.extractors.sql.experimental import SqlMeta

log = logging.getLogger(__name__)


def _is_in_table(token):
    return _match_on(token, [
        'FROM',
        'INNER JOIN',
        'JOIN',
        'FULL JOIN',
        'FULL OUTER JOIN',
        'LEFT JOIN',
        'LEFT OUTER JOIN',
        'RIGHT JOIN',
        'RIGHT OUTER JOIN'
    ])


def _is_out_table(token):
    return _match_on(token, ['INTO'])


def _match_on(token, keywords):
    return token.match(T.Keyword, values=keywords)


def _get_table(tokens, idx):
    idx, token = tokens.token_next(idx=idx)
    table = next(token.flatten()).value
    return idx, table


class SqlParser:
    """
    This class parses a SQL statement.
    """

    @staticmethod
    def parse(sql):
        if sql is None:
            raise ValueError("A sql statement must be provided")

        # Tokenize the SQL statement
        statements = sqlparse.parse(sql)

        # We assume only one statement in SQL
        tokens = TokenList(statements[0].tokens)
        log.debug(f"Tokenized {sql}: {tokens}")

        in_tables = []
        out_tables = []

        idx, token = tokens.token_next_by(t=T.Keyword)
        while token:
            if _is_in_table(token):
                idx, in_table = _get_table(tokens, idx)
                in_tables.append(in_table)
            elif _is_out_table(token):
                idx, out_table = _get_table(tokens, idx)
                out_tables.append(out_table)

            idx, token = tokens.token_next_by(t=T.Keyword, idx=idx)

        return SqlMeta(in_tables, out_tables)
