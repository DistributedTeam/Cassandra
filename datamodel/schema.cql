CREATE KEYSPACE cs4224 WITH replication = {'class':
  'SimpleStrategy', 'replication_factor': '1'} AND
  durable_writes = true;

CREATE TYPE address (
    street_1 text,
    street_2 text,
    city text,
    state text,
    zip text
);

CREATE TABLE warehouse_district (
    d_w_id smallint,
    d_id smallint,
    d_address address,
    d_name text,
    d_next_o_id int,
    d_tax float,
    d_ytd double,
    w_address address,
    w_name text,
    w_tax float,
    PRIMARY KEY ((d_w_id, d_id))
);

CREATE TABLE order_by_o_id (
    o_w_id smallint,
    o_d_id smallint,
    o_id int,
    o_all_local boolean,
    o_c_id int,
    o_carrier_id int,
    o_entry_d timestamp,
    o_ol_cnt int,
    PRIMARY KEY ((o_w_id, o_d_id), o_id)
);

CREATE MATERIALIZED VIEW order_by_c_id
  AS SELECT * FROM order_by_o_id
  WHERE o_w_id IS NOT NULL and o_d_id IS NOT NULL and
    o_c_id IS NOT NULL and o_id IS NOT NULL and
    o_entry_d IS NOT NULL and o_carrier_id IS NOT NULL
  PRIMARY KEY ((o_w_id, o_d_id), o_c_id, o_id);

CREATE TABLE order_line_item (
    ol_w_id smallint,
    ol_d_id smallint,
    ol_o_id int,
    ol_number int,
    i_name text,
    ol_amount double,
    ol_delivery_d timestamp,
    ol_dist_info text,
    ol_i_id int,
    ol_quantity int,
    ol_supply_w_id smallint,
    PRIMARY KEY ((ol_w_id, ol_d_id), ol_o_id, ol_number)
) WITH CLUSTERING ORDER BY (ol_o_id DESC, ol_number ASC);

CREATE TABLE stock_item (
    s_w_id smallint,
    s_i_id int,
    i_data text,
    i_im_id int,
    i_name text,
    i_price double,
    s_data text,
    s_dist_01 text,
    s_dist_02 text,
    s_dist_03 text,
    s_dist_04 text,
    s_dist_05 text,
    s_dist_06 text,
    s_dist_07 text,
    s_dist_08 text,
    s_dist_09 text,
    s_dist_10 text,
    s_order_cnt int,
    s_quantity int,
    s_remote_cnt int,
    s_ytd double,
    PRIMARY KEY ((s_w_id, s_i_id))
);

CREATE TABLE customer (
    c_w_id smallint,
    c_d_id smallint,
    c_id int,
    c_address address,
    c_balance double,
    c_credit text,
    c_credit_lim double,
    c_data text,
    c_delivery_cnt int,
    c_discont float,
    c_first text,
    c_middle text,
    c_last text,
    c_payment_cnt int,
    c_phone text,
    c_since timestamp,
    c_ytd_payment float,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);

CREATE MATERIALIZED VIEW customer_balance
  AS SELECT * FROM customer
  WHERE c_w_id IS NOT NULL and c_d_id IS NOT NULL and
    c_balance IS NOT NULL and c_id IS NOT NULL and
    c_first IS NOT NULL and c_middle IS NOT NULL and
    c_last IS NOT NULL
  PRIMARY KEY ((c_w_id, c_d_id), c_balance, c_id);