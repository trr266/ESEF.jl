using Chain
using DataFrameMacros

strip_wikidata_prefix(df, cols) = @chain df @transform(cols = @passmissing replace({cols}, "http://www.wikidata.org/entity/" => ""))
