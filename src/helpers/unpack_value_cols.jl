using Chain
using DataFrameMacros

unpack_value_cols(df, cols) = 
    nrow(df) == 0 ? df : @chain df @transform(cols = @passmissing {cols}["value"])
