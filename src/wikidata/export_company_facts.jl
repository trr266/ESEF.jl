using HTTP
using JSON
using DataFrames
using DataFrameMacros
using Chain
using Mustache
using Memoization

function basic_wikidata_preprocessing(df)
    df = @chain df begin
        @transform(:wikidata_uri = :entity["value"])
        @transform(:company_label = @passmissing :entityLabel["value"])
        @transform(:isin_id = @passmissing :isin_value["value"])
        @transform(:country_uri = @passmissing :country["value"])
        @transform(:country = @passmissing :countryLabel["value"])
        @transform(:country_alpha_2 = @passmissing :country_alpha_2["value"])
        @transform(:isin_alpha_2 = @passmissing first(:isin_id, 2))
        @transform(:lei_id = @passmissing :lei_value["value"])
        @groupby(
            :wikidata_uri,
            :company_label,
            :country,
            :country_uri,
            :country_alpha_2,
            :isin_id,
            :isin_alpha_2,
            :lei_id
        )
        @select(
            :wikidata_uri,
            :company_label,
            :country,
            :country_uri,
            :country_alpha_2,
            :isin_id,
            :isin_alpha_2,
            :lei_id,
        )
    end

    # Add in country names
    country_lookup = get_country_codes()

    df = @chain df begin
        leftjoin(
            _,
            (@chain country_lookup @select(:region, :country_alpha_2));
            on=:country_alpha_2,
            matchmissing=:notequal,
        )
        leftjoin(
            _,
            (@chain country_lookup @select(
                :isin_alpha_2 = :country_alpha_2,
                :isin_country = :country,
                :isin_region = :region
            ));
            on=:isin_alpha_2,
            matchmissing=:notequal,
        )
    end

    return df = @chain df @transform(
        :esef_regulated = esef_regulated(:isin_region, :region)
    )
end

function get_non_lei_isin_companies_wikidata()
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/
    q_path = joinpath(@__DIR__, "..", "queries", "wikidata_non_lei_isin_firms.sparql")
    df = @chain q_path query_wikidata()
    df = @chain df @transform(:lei_id = nothing)
    df = basic_wikidata_preprocessing(df)

    return df
end

@memoize function get_lei_companies_wikidata()
    # TODO: figure out why entries are not unique...
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "queries", "wikidata_lei_entities.sparql")
    df = @chain q_path query_wikidata()
    df = basic_wikidata_preprocessing(df)

    return df
end

function get_company_facts()
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "queries", "wikidata_company_lei_isin_facts.sparql")
    df = @chain q_path query_wikidata() @select(
        :subject = :sub["value"], :predicate = :p["value"], :object = :o["value"]
    )
    return df
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
        q_path = joinpath(@__DIR__, "..", "queries", "wikidata_company_search.sparql")
        df = @chain q_path query_wikidata(params=Dict("company_name" => company_name))

        if nrow(df) == 0
            return DataFrame()
        end

        df = @chain df begin
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
