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

table! {
  sync_ticks (id) {
      id -> Int4,
      token0_symbol -> Varchar,
      token1_symbol -> Varchar,
      token0_address -> Varchar,
      token1_address -> Varchar,
      block_number -> Int8,
      address -> Varchar,
      reserve0 -> Float8,
      reserve1 -> Float8,
      token0_usd_price -> Float8,
      token1_usd_price -> Float8,
  }
}

table! {
  swap_ticks (id) {
      id -> Int4,
      token0_symbol -> Varchar,
      token1_symbol -> Varchar,
      token0_address -> Varchar,
      token1_address -> Varchar,
      block_number -> Int8,
      address -> Varchar,
      sender -> Varchar,
      amount0_in -> Float8,
      amount0_out -> Float8,
      amount1_in -> Float8,
      amount1_out -> Float8,
      token0_usd_price -> Float8,
      token1_usd_price -> Float8,
  }
}

table! {
  liquidity_ticks (id) {
      id -> Int4,
      token0_symbol -> Varchar,
      token1_symbol -> Varchar,
      token0_address -> Varchar,
      token1_address -> Varchar,
      block_number -> Int8,
      address -> Varchar,
      sender -> Varchar,
      amount0 -> Float8,
      amount1 -> Float8,
      token0_usd_price -> Float8,
      token1_usd_price -> Float8,
  }
}
