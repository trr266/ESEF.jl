using Chain
using DataFrameMacros

function strip_wikidata_prefix(df, cols)
    @chain df @transform(
        cols = @passmissing replace({cols}, "http://www.wikidata.org/entity/" => "")
    )
end

function rehydrate_uri_entity(uri)
    return HTTP.unescapeuri(replace(uri, "http://example.org/" => ""))
end
