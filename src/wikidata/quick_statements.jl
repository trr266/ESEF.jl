using Chain

function build_wikidata_record(lei_data, wd_country_lookup)
    @chain lei_data begin
        extract_lei_information()
        generate_quick_statement(wd_country_lookup)
    end
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
        _[1, 1]
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
        lei_str = join(leis[i:min(i + 200, length(leis))], ",")
        i += 200

        lei_data = get_lei_data(lei_str)["data"]
        append!(
            qs_statements,
            [build_wikidata_record(lei_data_, wd_country_lookup) for lei_data_ in lei_data],
        )
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

function get_iso_wikidata_lookup()
    q_path = joinpath(@__DIR__, "..", "..", "queries", "wikidata_country_iso_2.sparql")
    df = @chain q_path begin
        query_wikidata()
        @transform(
            :country = :country["value"], :country_alpha_2 = :country_alpha_2["value"]
        )
        @select(
            :country = replace(:country, "http://www.wikidata.org/entity/" => ""),
            :country_alpha_2
        )
    end

    return df
end

function get_full_wikidata_leis()
    q_path = joinpath(@__DIR__, "..", "..", "wikidata_pure_lei.sparql")
    df = @chain q_path begin
        query_wikidata()
        @transform(
            :entity = :entity["value"],
            :entityLabel = :entityLabel["value"],
            :lei_value = :lei_value["value"]
        )
    end

    return df
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
