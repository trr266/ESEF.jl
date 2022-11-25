using HTTP
using JSON
using DataFrames
using DataFrameMacros
using Chain
using Mustache
using Memoization

# TODO: Figure out where this is needed!
# TODO: Drop excess functions...
# Add in country names
# country_lookup = get_country_codes()
# (@chain country_lookup @select(
#     :isin_alpha_2 = :country_alpha_2,
#     :isin_country = :country,
#     :isin_region = :region
# ));


function get_non_lei_isin_companies_wikidata()
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
            :entity,
            :entityLabel,
            :isin_value,
            :country,
            :countryLabel,
            :country_alpha_2,
        ])
        @transform(:isin_alpha_2 = first(:isin_value, 2))
    end
end




@memoize function get_lei_companies_wikidata()
    # TODO: figure out why entries are not unique...
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "lei_entities.sparql")
    @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols([
            :country
            :countryLabel
            :country_alpha_2
            :entity
            :entityLabel
            :isin_value
            :lei_value
        ])
    end
end

function get_company_facts()
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "company_lei_isin_facts.sparql"
    )
    @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols([:sub, :predicate, :o])
        @transform(:object = :o)
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
        query_wikidata_sparql(params=Dict("property" => property))
        unpack_value_cols([:subject, :subjectLabel, :object])
        @transform(:predicate = "http://www.wikidata.org/entity/$property")
    end
end

function esef_regulated(isin_region, country_region)
    if ismissing(isin_region) && ismissing(country_region)
        return missing
    elseif ismissing(isin_region)
        return country_region == "Europe"
    elseif ismissing(country_region)
        return isin_region == "Europe"
    else
        return (country_region == "Europe") || (isin_region == "Europe")
    end
end

function lookup_company_by_name(company_name)
    try
        q_path = joinpath(
            @__DIR__, "..", "..", "queries", "wikidata", "company_search.sparql"
        )
        df = @chain q_path query_wikidata_sparql(
            params=Dict("company_name" => company_name)
        )

        if nrow(df) == 0
            return DataFrame()
        end

        df = @chain df begin
            # TODO: use unpack_value_cols
            @transform(
                :wikidata_uri = :company["value"],
                :company_label = :companyLabel["value"],
                :company_description = :companyDescrip["value"]
            )
            @groupby(:wikidata_uri)
            @combine(
                :company_label = :company_label[1],
                :company_description = :company_description[1]
            )
            @select(:wikidata_uri, :company_label, :company_description)
        end

        return df
    catch e
        return DataFrame()
    end
end

function get_full_wikidata_leis()
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "pure_lei.sparql")
    @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols([
            :entity,
            :entityLabel,
            :lei_value,
        ])
    end
end
