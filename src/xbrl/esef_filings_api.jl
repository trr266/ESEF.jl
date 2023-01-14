using HTTP
using Chain
using DataFrames
using DataFrameMacros
using CSV
using JSON
using Memoization

function pluck_xbrl_json(url)
    r = HTTP.get(url)

    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end

    finished_facts = DataFrame()

    for (k, fact) in raw_data["facts"]
        flat_fact = rec_flatten_dict(fact)

        if haskey(flat_fact, "dimensions.entity")
            flat_fact["dimensions.entity"] = replace(
                flat_fact["dimensions.entity"], "scheme:" => ""
            )
        end

        for (k_subfact, v_subfact) in flat_fact
            push!(
                finished_facts,
                NamedTuple{(:subject, :predicate, :object)}([
                    k, k_subfact, string(v_subfact)
                ]),
            )
        end
    end

    return finished_facts
end

# TODO: Extract XBRL facts from items where "xbrl-json" key is populated.
# 2594003JTXPYO8NOG018/2020-12-31/ESEF/PL/0
# https://filings.xbrl.org/2594003JTXPYO8NOG018/2020-12-31/ESEF/PL/0/enea-2020-12-31.json

@memoize function get_esef_xbrl_filings()
    xbrl_esef_index_endpoint = "https://filings.xbrl.org/index.json"
    r = HTTP.get(xbrl_esef_index_endpoint)

    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end

    df = DataFrame()
    row_names = (
        :key,
        :entity_name,
        :country_alpha_2,
        :date,
        :filing_key,
        :error_count,
        :error_codes,
        :xbrl_json_path,
    )

    df_error = DataFrame()

    # Parse XBRL ESEF Index Object
    for (d_key, d_value) in raw_data
        entity_name = d_value["entity"]["name"]

        for (filing_key, filing_value) in d_value["filings"]
            error_payload = filing_value["errors"]
            error_count = length(error_payload)
            error_codes = [d["code"] for d in error_payload]

            country = filing_value["country"]
            date = filing_value["date"]

            xbrl_json_path = nothing

            if haskey(filing_value, "xbrl-json")
                xbrl_json_path = filing_value["xbrl-json"]
                xbrl_json_path = xbrl_json_path == "" ? nothing : xbrl_json_path
            end

            new_row = NamedTuple{row_names}([
                d_key,
                entity_name,
                country,
                date,
                filing_key,
                error_count,
                error_codes,
                xbrl_json_path,
            ])
            push!(df, new_row; promote=true)

            for error_code in error_codes
                push!(df_error, NamedTuple{(:key, :error_code)}([d_key, error_code]))
            end
        end
    end

    df = @transform! df @subset(
        begin
            :country_alpha_2 == "CS"
        end
    ) begin
        :country_alpha_2 = "CZ"
    end

    # Add in country names
    country_lookup = get_wikidata_country_iso2_lookup()
    # TODO: Make sure Czechia is joined correctly
    df = @chain df begin
        leftjoin(_, country_lookup; on=:country_alpha_2)
    end

    return df, df_error
end

function calculate_country_rollup(df)
    country_rollup = @chain df begin
        @subset(!ismissing(:countryLabel))
        @groupby(:countryLabel)
        @combine(:report_count = length(:countryLabel))
        @transform(:report_count = coalesce(:report_count, 0))
        @sort(:report_count; rev=true)
    end
    return country_rollup
end
