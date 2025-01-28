using HTTP
using JSON
using DataFrames
using CSV
using Chain
using DataFrameMacros

function get_lei_data(lei::Vector)
    return get_lei_data(join(lei, ","))
end

function get_lei_data(lei::String)
    sleep(1) # Rate limited to 1 request per second
    query = Dict("filter[lei]" => lei, "page[size]" => 200)

    data = @chain "https://api.gleif.org/api/v1/lei-records" begin
        HTTP.get(; query=query)
        @aside @assert(_.status == 200)
        _.body
        String
        JSON.parse
        _["data"]
    end

    return data
end

function get_lei_names(lei_entry)
    lei_legal_name = missing
    legal_name_entry = lei_entry["attributes"]["entity"]["legalName"]
    if isa(legal_name_entry, Dict)
        lei_legal_name = legal_name_entry["name"]
    end

    lei_other_names = []

    other_names_entry = lei_entry["attributes"]["entity"]["otherNames"]
    if length(other_names_entry) > 0
        lei_other_names = [o["name"] for o in other_names_entry]
    end

    return lei_legal_name, lei_other_names
end

function get_isin_data(lei)
    sleep(1) # Rate limited to 1 request per second
    d = @chain "https://api.gleif.org/api/v1/lei-records/$lei/isins" begin
        HTTP.get
        @aside @assert(_.status == 200)
        _.body
        String
        JSON.parse
    end

    d = [i["attributes"]["isin"] for i in d["data"] if i["attributes"]["lei"] == lei]

    return d
end

function extract_lei_information(lei_data)
    d_out = Dict{String,Any}()

    d_out["lei"] = lei_data["id"]

    if haskey(lei_data["relationships"], "isins")
        d_out["isins"] = sort(get_isin_data(d_out["lei"]))
    else
        d_out["isins"] = []
    end

    d_out["entity_names"] = [
        Dict(
            "name" => lei_data["attributes"]["entity"]["legalName"]["name"],
            "language" => lei_data["attributes"]["entity"]["legalName"]["language"][1:2],
        ),
    ]

    d_out["country"] = lei_data["attributes"]["entity"]["legalAddress"]["country"]
    return d_out
end
