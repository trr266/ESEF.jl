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

    @chain "https://api.gleif.org/api/v1/lei-records" begin
        HTTP.get(; query=query)
        @aside @assert(_.status == 200)
        _.body
        String
        JSON.parse
        _["data"]
    end
end

function get_lei_names(lei_data)
    lei_legal_name = missing
    if "legalName" in keys(lei_data["attributes"]["entity"])
        lei_legal_name = lei_data["attributes"]["entity"]["legalName"]["name"]
    end

    lei_other_name = missing

    if "otherNames" in keys(lei_data["attributes"]["entity"])
        lei_other_name = lei_data["attributes"]["entity"]["otherNames"][1]["name"]
    end

    return lei_legal_name, lei_other_name
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
        d_out["isins"] = get_isin_data(d_out["lei"])
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
