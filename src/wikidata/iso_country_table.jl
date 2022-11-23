using Chain
using Memoization


@memoize function get_wikidata_country_iso2_lookup()
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata_country_iso_2.sparql")
    df = @chain q_path begin
        query_wikidata()
        @transform(:country = :country["value"], :country_alpha_2 = :country_alpha_2["value"])
        @select(:country = replace(:country, "http://www.wikidata.org/entity/" => ""), :country_alpha_2)
    end

    return df
end
