using HTTP
using JSON
using DataFrames
using DataFrameMacros
using Chain
using Mustache
using Memoization


function basic_wikidata_preprocessing(df)
    df = @chain df begin
        unpack_value_cols([
            :entity,
            :entityLabel,
            :isin_value,
            :country,
            :countryLabel,
            :country_alpha_2,
            :lei_value
        ])
        @transform(:isin_alpha_2 = @passmissing first(:isin_id, 2))
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
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "non_lei_isin_firms.sparql"
    )
    @chain q_path begin
        query_wikidata_sparql()
        @transform(:lei_id = nothing)
        # basic_wikidata_preprocessing()
    end
end

df = get_non_lei_isin_companies_wikidata()

@test unpack_value_cols(df, [:a])[1, :a] == 1

@memoize function get_lei_companies_wikidata()
    # TODO: figure out why entries are not unique...
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata", "lei_entities.sparql")
    df = @chain q_path query_wikidata_sparql()
    df = basic_wikidata_preprocessing(df)

    return df
end

function get_company_facts()
    # TODO: swap this out for artifacts https://pkgdocs.julialang.org/v1/creating-packages/=
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "company_lei_isin_facts.sparql"
    )
    df = @chain q_path query_wikidata_sparql() @select(
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
    df = @chain q_path begin
        query_wikidata_sparql()
        @transform(
            :entity = :entity["value"],
            :entityLabel = :entityLabel["value"],
            :lei_value = :lei_value["value"]
        )
    end

    return df
end
