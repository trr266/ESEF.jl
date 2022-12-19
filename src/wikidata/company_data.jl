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


@memoize function get_facts_for_property(property)
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

function get_entities_which_are_instance_of_object(object)
    """
    Get all entities which are an instance of a given object.
    """
    q_path = joinpath(
        @__DIR__, "..", "..", "queries", "wikidata", "facts_for_instance_of_object.sparql"
    )
    return @chain q_path begin
        query_wikidata_sparql(; params=Dict("object" => object))
        unpack_value_cols([:subject, :subjectLabel])
        @transform(
            :predicate = "http://www.wikidata.org/entity/P31",
            :object = "http://www.wikidata.org/entity/$object"
        )
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

wikidata_accounting_properties = Dict(:lei => "P1278", :isin => "P946")
wikidata_accounting_objects = Dict(:business => "Q4830453")#, :enterprise => "Q6881511")


function get_full_wikidata_leis()
    return get_facts_for_property(wikidata_accounting_properties[:lei])
end

function get_full_wikidata_isins()
    return get_facts_for_property(wikidata_accounting_properties[:isin])
end


function get_accounting_facts()
    df_properties = Dict(
        k => get_facts_for_property(v) for (k, v) in wikidata_accounting_properties
    )

    # TODO: Replace this with wikidata dump & filter!
    # df_objects = Dict(
    #     k => get_entities_which_are_instance_of_object(v) for (k, v) in wikidata_accounting_objects
    # )

    df_properties = reduce(vcat, df_properties)
    # df_objects = reduce(vcat, df_objects)

    return vcat(df_properties, df_objects)
end

function get_wikidata_economic_and_accounting_concepts()
    """
    Get all economic concepts from Wikidata.
    """
    q_path = joinpath(
        @__DIR__,
        "..",
        "..",
        "queries",
        "wikidata",
        "economic_and_accounting_concepts.sparql",
    )
    return @chain q_path begin
        query_wikidata_sparql()
        unpack_value_cols([:concept, :conceptLabel])
        @groupby(:concept)
        @select(:concept, :conceptLabel)
    end
end
