using Chain
using DataFrameMacros

unpack_value_cols(df, cols) = @chain df @transform(cols = @passmissing {cols}["value"])


