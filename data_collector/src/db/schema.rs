use diesel::table;

table! {
  blocks (id) {
      id -> Int4,
      block_number -> Int8,
      timestamp -> Int8,
      gas_price -> Float8,
      gas_used -> Int8,
  }
}

table! {
  logs (id) {
      id -> Int4,
      log_type -> Int4,
      block_number -> Int8,
      address -> Varchar,
      data1 -> Nullable<Varchar>,
      data2 -> Nullable<Varchar>,
      data3 -> Nullable<Varchar>,
      data4 -> Nullable<Varchar>,
      data5 -> Nullable<Varchar>,
  }
}

table! {
  cex_data (id) {
      id -> Int4,
      platform_slug -> Varchar,
      token_address -> Varchar,
      symbol -> Varchar,
  }
}

table! {
  pools (id) {
      id -> Int4,
      address -> Varchar,
      token0 -> Varchar,
      token1 -> Varchar,
  }
}
