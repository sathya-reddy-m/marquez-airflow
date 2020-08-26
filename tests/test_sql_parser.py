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

from marquez_airflow.sql.parser import SqlParser


def test_parse_simple_select():
    sql_meta = SqlParser.parse('SELECT * FROM discounts;')
    assert sql_meta.in_tables == ['discounts']
    assert sql_meta.out_tables == []


def test_parse_simple_insert_into():
    sql_meta = SqlParser.parse('''
        INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on, orders_placed)
        SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
               order_placed_on, COUNT(*) AS orders_placed
          FROM top_delivery_times
         GROUP BY order_placed_on;
        '''
    )

    assert sql_meta.in_tables == ['top_delivery_times']
    assert sql_meta.out_tables == ['popular_orders_day_of_week']


def test_parse_complex_insert_into():
    sql_meta = SqlParser.parse('''
        INSERT INTO orders_7_days (order_id, placed_on, discount_id, menu_id,
                    restaurant_id, menu_item_id, category_id)
        SELECT o.id AS order_id, o.placed_on, o.discount_id, m.id AS menu_id,
               m.restaurant_id, mi.id AS menu_item_id, c.id AS category_id
          FROM orders AS o
         INNER JOIN menu_items AS mi
            ON mi.id = o.menu_item_id
         INNER JOIN categories AS c
            ON c.id = mi.category_id
         INNER JOIN menus AS m
            ON m.id = c.menu_id
         WHERE o.placed_on >= NOW() - interval '7 days';
        '''
    )

    assert sql_meta.in_tables == ['orders', 'menu_items', 'categories', 'menus']
    assert sql_meta.out_tables == ['orders_7_days']
