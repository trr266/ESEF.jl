using HTTP
using JSON
using DataFrames
using DataFrameMacros
using Chain
using Mustache
using Memoization

function get_companies_with_isin_without_lei_wikidata()
    """
    Get a list of companies that have an ISIN but not a LEI from Wikidata.
    """
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "non_lei_isin_firms.sparql"
    )
    @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols([
            :entity, :entityLabel, :isin_value, :country, :countryLabel, :country_alpha_2
        ])
        @transform(:isin_alpha_2 = first(:isin_value, 2))
    end
end

@memoize function get_companies_with_leis_wikidata()
    # TODO: fix non-unique entries in Wikidata
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "lei_entities.sparql")
    @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols(
            [
                :country
                :countryLabel
                :country_alpha_2
                :entity
                :entityLabel
                :isin_value
                :lei_value
            ]
        )
    end
end

function get_facts_for_property(property)
    """
    Get all facts which use a given property.
    """
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "facts_for_property.sparql"
    )
    @chain q_path begin
        query_wikidata_sparql(; params=Dict("property" => property))
        unpack_value_cols([:subject, :subjectLabel, :object])
        @transform(:predicate = "http://www.wikidata.org/entity/$property")
    end
end

function esef_regulated(isin_country::String, incorporation_country::String)
    esma_countries = get_esma_regulated_countries()
    return (isin_country ∈ esma_countries) || (incorporation_country ∈ esma_countries)
end

function esef_regulated(isin_country::Vector, incorporation_country)
    esma_countries = get_esma_regulated_countries()
    return any(isin_country .∈ esma_countries) || (incorporation_country ∈ esma_countries)
end

function search_company_by_name(company_name)
    try
        q_path = joinpath(
            @__DIR__, "..", "..", "queries", "wikidata", "company_search.sparql"
        )
        @chain q_path begin
            query_wikidata_sparql(; params=Dict("company_name" => company_name))
            unpack_value_cols([:company, :companyLabel, :companyDescrip])
            @groupby(:company)
            @combine(:companyLabel = :companyLabel[1], :companyDescrip = :companyDescrip[1])
            @select(:company, :companyLabel, :companyDescrip)
        end
    catch e
        return DataFrame()
    end
end

function get_full_wikidata_leis()
    return get_facts_for_property("P1278")
end

function get_company_facts()
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "company_lei_isin_facts.sparql"
    )

    return get_full_wikidata_leis()
end
