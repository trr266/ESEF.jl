using DataFrames
using Chain
using DataFrameMacros

function get_property_facts(property::String)
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "fast_property_fact_lookup.sparql")
    df = @chain q_path begin
        query_wikidata_sparql()
        @select(
            :subject = :sub["value"],
            :predicate = property,
            :object = :o["value"]
        )
    end
    return df
end
