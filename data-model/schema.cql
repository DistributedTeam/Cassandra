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
    d_tax double,
    w_address address,
    w_name text,
    w_tax double,
    PRIMARY KEY ((d_w_id, d_id))
);

CREATE TABLE warehouse_district_stats (
    d_w_id smallint,
    d_id smallint,
    d_next_o_id counter,
    d_ytd counter,
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
    PRIMARY KEY ((o_w_id, o_d_id, o_id))
);

CREATE TABLE delivery_transaction (
    dt_w_id smallint,
    dt_d_id smallint,
    dt_min_ud_o_id counter,
    PRIMARY KEY ((dt_w_id, dt_d_id))
);


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
    PRIMARY KEY ((ol_w_id, ol_d_id, ol_o_id), ol_number)
);

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
    PRIMARY KEY ((s_w_id, s_i_id))
);

CREATE TABLE stock_item_stats (
    s_w_id smallint,
    s_i_id int,
    s_order_cnt counter,
    s_quantity counter,
    s_remote_cnt counter,
    s_ytd counter,
    PRIMARY KEY ((s_w_id, s_i_id))
);

CREATE TABLE customer_partial (
    c_w_id smallint,
    c_d_id smallint,
    c_id int,
    c_first text,
    c_middle text,
    c_last text,
    c_balance double,
    c_last_order int,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);

CREATE TABLE customer (
    c_w_id smallint,
    c_d_id smallint,
    c_id int,
    c_address address,
    c_credit text,
    c_credit_lim double,
    c_data text,
    c_discount double,
    c_first text,
    c_middle text,
    c_last text,
    c_phone text,
    c_since timestamp,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);

CREATE TABLE customer_stats (
    c_w_id smallint,
    c_d_id smallint,
    c_id int,
    c_balance counter,
    c_delivery_cnt counter,
    c_payment_cnt counter,
    c_ytd_payment counter,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);


CREATE MATERIALIZED VIEW customer_balance
  AS SELECT * FROM customer_partial
  WHERE c_w_id IS NOT NULL and c_d_id IS NOT NULL and
    c_balance IS NOT NULL and c_id IS NOT NULL and
    c_first IS NOT NULL and c_middle IS NOT NULL and
    c_last IS NOT NULL
  PRIMARY KEY ((c_w_id, c_d_id), c_balance, c_id);

ALTER TABLE warehouse_district WITH
  compaction = { 'class' :  'LeveledCompactionStrategy'  };

ALTER TABLE stock_item WITH
  compaction = { 'class' :  'LeveledCompactionStrategy'  };

ALTER TABLE customer WITH
  compaction = { 'class' :  'LeveledCompactionStrategy'  }