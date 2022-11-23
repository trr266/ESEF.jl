using Chain
using DataFrames
using DataFrameMacros

function generate_quick_statement_from_lei_obj(gleif_lei_obj)
    df_quick_statements = DataFrame(; predicate=String[], object=Int[])

    # Language-tagged Primary Company Name 
    push!(df_quick_statements,
        [
            gleif_lei_obj["entity_names"][1]["language"],
            gleif_lei_obj["entity_names"][1]["name"]
        ])

    # LEI
    push!(df_quick_statements, ["P1278", gleif_lei_obj["lei"]])
    
    # Legal Jurisdiction (ISO Wikidata is memoized)
    @chain get_wikidata_country_iso2_lookup() begin
        @subset(:country_alpha_2 == gleif_lei_obj["country"])
        _[1,1]
        @aside push!(df_quick_statements, ["P17", _])
    end
    
    # Associated ISINs
    for i in d["isins"]
        push!(df_quick_statements, ["P946", i])
    end

    # Instance of Enterprise
    push!(df_quick_statements, ["P31", "Q6881511"])

    qs_statement = @chain df_quick_statements
        @bycols build_quick_statement(:predicate, :object)
    end

    return qs_statement
end

function build_wikidata_record(lei)
    @chain lei begin
        get_lei_data()
        extract_lei_information()
        generate_quick_statement_from_lei_obj()
    end
end

function import_missing_leis_to_wikidata(leis)
    i = 1
    qs_statements = []
    while i < length(leis)
        lei_str = join(leis[i:min(i+200, length(leis))], ",")
        i += 200
    
        lei_data = get_lei_data(lei_str)["data"]
        append!(qs_statements, [build_wikidata_record(lei_data_) for lei_data_ in lei_data])
    end
    
    qs_statements_str = join(qs_statements, "\n")

    open("quick_statement.txt", "w") do f
        write(f, qs_statements_str)
    end
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
