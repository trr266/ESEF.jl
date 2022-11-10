using HTTP
using JSON
using DataFrames
using CSV

function get_lei_data(lei)
    r = HTTP.get("https://api.gleif.org/api/v1/lei-records/$(lei)")

    @assert(r.status == 200)

    d = JSON.parse(String(r.body))
end

function get_lei_names(lei)
    d = get_lei_data(lei)
    lei_legal_name = missing
    try
        lei_legal_name = d["data"]["attributes"]["entity"]["legalName"]["name"]
    catch
    end

    lei_other_name = missing

    try
        lei_other_name = d["data"]["attributes"]["entity"]["otherNames"][1]["name"]
    catch
    end

    return lei_legal_name, lei_other_name
end

function get_isin_data(lei)
    r = HTTP.get("https://api.gleif.org/api/v1/lei-records/$lei/isins")

    @assert(r.status == 200)

    d = JSON.parse(String(r.body))

    d = [i["attributes"]["isin"] for i in d["data"] if i["attributes"]["lei"] == lei]

    return d
end

function extract_lei_information(lei)
    d = get_lei_data(lei)

    d_out = Dict{String, Any}()

    d_out["lei"] = d["data"]["id"]

    if haskey(d["data"]["relationships"], "isins")
        d_out["isins"] = get_isin_data(lei)
    end


    d_out["name"] = d["data"]["attributes"]["entity"]["legalName"]

    d_out["country"] = d["data"]["attributes"]["entity"]["legalAddress"]["country"]
    return d_out
end

function build_wikidata_record(lei)
    d = extract_lei_information(lei)

    return d
end

# lei = "259400GFZ573WP2RBL28"
# d_ = build_wikidata_record(lei)

# d_
# # First line blank to create item
# qid,Len,Den,P31
# ,Regina Phalange,fictional character,Q95074


# d_out = Dict{String, Any}()
# d_out["L$(d_["name"]["language"][1:2])"] = d_["name"]["name"]
# d_out["P1278"] = d_["lei"]
# d_out["P17"] = d_["country"]
# d_out["P31"] = "Q6881511"

# df = DataFrame(d_out)
# insertcols!(df, 1, :qid => "")
# CSV.write("test.csv", df)
