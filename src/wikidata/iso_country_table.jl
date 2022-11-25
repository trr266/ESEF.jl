using Chain
using Memoization
using DataFrameMacros

@memoize function get_wikidata_country_iso2_lookup()
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "country_iso_2.sparql")
    @chain q_path begin
        query_wikidata_sparql()
        @transform(
            :country = :country["value"], :country_alpha_2 = :country_alpha_2["value"]
        )
        strip_wikidata_prefix([:country])
        @select(
            :country,
            :country_alpha_2
        )
    end
end
