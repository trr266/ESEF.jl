using HTTP
using JSON
using DataFrames
using CSV
using Chain
using DataFrameMacros

function get_lei_data(lei)
    sleep(1.01) # Rate limited to 1 request per second
    r = HTTP.get("https://api.gleif.org/api/v1/lei-records";
        query=Dict("filter[lei]" => lei,
        "page[size]"=> 200))

    @assert(r.status == 200)

    d = JSON.parse(String(r.body))
end

function get_lei_names(lei_data)
    lei_legal_name = missing
    try
        lei_legal_name = lei_data["attributes"]["entity"]["legalName"]["name"]
    catch
    end

    lei_other_name = missing

    try
        lei_other_name = lei_data["attributes"]["entity"]["otherNames"][1]["name"]
    catch
    end

    return lei_legal_name, lei_other_name
end

function get_isin_data(lei)
    sleep(1.01) # Rate limited to 1 request per second
    r = HTTP.get("https://api.gleif.org/api/v1/lei-records/$lei/isins")

    @assert(r.status == 200)

    d = JSON.parse(String(r.body))

    d = [i["attributes"]["isin"] for i in d["data"] if i["attributes"]["lei"] == lei]

    return d
end

function extract_lei_information(lei_data)
    d_out = Dict{String, Any}()

    d_out["lei"] = lei_data["id"]

    if haskey(lei_data["relationships"], "isins")
        d_out["isins"] = get_isin_data(d_out["lei"])
    else
        d_out["isins"] = []
    end

    d_out["name"] = lei_data["attributes"]["entity"]["legalName"]

    d_out["country"] = lei_data["attributes"]["entity"]["legalAddress"]["country"]
    return d_out
end

function build_wikidata_record(lei_data, wd_country_lookup)
    @chain lei_data begin
        extract_lei_information()
        generate_quick_statement(wd_country_lookup)
    end
end

function get_iso_wikidata_lookup()
    q_path = joinpath(@__DIR__, "..", "queries", "wikidata_country_iso_2.sparql")
    df = @chain q_path begin
        query_wikidata()
        @transform(:country = :country["value"], :country_alpha_2 = :country_alpha_2["value"])
        @select(:country = replace(:country, "http://www.wikidata.org/entity/" => ""), :country_alpha_2)
    end

    return df
end

function get_full_wikidata_leis()
    q_path = joinpath(@__DIR__, "..", "queries", "wikidata_pure_lei.sparql")
    df = @chain q_path begin
        query_wikidata()
        @transform(:entity = :entity["value"], :entityLabel = :entityLabel["value"], :lei_value = :lei_value["value"])
    end

    return df
end

function generate_quick_statement(d, wd_country_lookup)
    d_label = """
    LAST\tL$(d["name"]["language"][1:2])\t"$(d["name"]["name"])"
    """

    d_lei = """
    LAST\tP1278\t"$(d["lei"])"
    """

    wd_country = @chain wd_country_lookup begin
        @subset(:country_alpha_2 == d["country"])
        _[1,1]
    end
    
    d_country = """
    LAST\tP17\t$(wd_country)
    """

    d_isin = ["LAST\tP946\t\"$(i)\"" for i in d["isins"]]

    d_enterprise = "LAST\tP31\tQ6881511"

    qs_statement = join(["CREATE", d_label, d_lei, d_country, d_isin...], "\n")

    return qs_statement
end

function import_missing_leis_to_wikidata(leis)
    wd_country_lookup = get_iso_wikidata_lookup()
    i = 1
    qs_statements = []
    while i < length(leis)
        lei_str = join(leis[i:min(i+200, length(leis))], ",")
        i += 200
    
        lei_data = get_lei_data(lei_str)["data"]
        append!(qs_statements, [build_wikidata_record(lei_data_, wd_country_lookup) for lei_data_ in lei_data])
    end
    
    qs_statements_str = join(qs_statements, "\n")

    open("quick_statement.txt", "w") do f
        write(f, qs_statements_str)
    end
end

function compose_merge_statement(wd_entity_vector)
    qs_statements = []
    for merge_ in wd_entity_vector[2:end]
        push!(qs_statements, "MERGE\t$(wd_entity_vector[1])\t$(merge_)")
    end

    return qs_statements
end


# TODO: Write tests
# leis = df[:, :key]

# import_missing_leis_to_wikidata(leis)
dupe_leis = @chain df begin
    _[findall(nonunique(_, :lei_value)), :lei_value]
end

function merge_duplicate_wikidata_on_leis()
    df = get_full_wikidata_leis()

    dupe_leis = @chain df begin
        _[findall(nonunique(_, :lei_value)), :lei_value]
    end
    
    qs_statements_str = @chain df begin
        @subset(:lei_value ∈ dupe_leis)
        @transform(:entity = replace(:entity, "http://www.wikidata.org/entity/" => ""))
        @groupby(:lei_value)
        @combine(:merge_statement = compose_merge_statement(:entity))
        join(_[:, :merge_statement], "\n")
    end
    
    open("quick_statement.txt", "w") do f
        write(f, qs_statements_str)
    end    
end
