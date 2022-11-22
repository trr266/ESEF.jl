using AlgebraOfGraphics
using CairoMakie
using Chain
using Colors
using CSV
using DataFrameMacros
using DataFrames
using Dates
using HTTP
using JSON
using Statistics
using URIParser

function esef_wikidata_mini_analysis()
    df_wikidata_lei = get_lei_companies_wikidata()

    # Add further data checks
    # Check only minimal number of firms where country is missing (e.g. EU, ersatz XC/XY/XS, or incorrect 00, 23)
    @assert((@chain df_wikidata_lei @subset(ismissing(:esef_regulated)) nrow()) < 1e3)
    # @assert((@chain df_wikidata_isin @subset(ismissing(:esef_regulated)) nrow()) < 10)

    # Drop firms where country is missing
    # df_wikidata_isin = @chain df_wikidata_isin @subset(:esef_regulated; skipmissing=true) 

    # TODO: Look at this group of companies who are subject to regulation, but not available via XBRL
    # @chain df_wikidata_isin @subset(:country == "Germany"; skipmissing=true)

    df, df_error = get_esef_xbrl_filings()

    df = @chain df begin
        leftjoin(
            df_wikidata_lei; on=(:key => :lei_id), matchmissing=:notequal, makeunique=true
        )
    end

    df_1 = @chain df begin
        @subset(ismissing(:wikidata_uri))
        @select(:key, :entity_name, :company_label)
    end

    # @assert(nrow(df_1) == 0, "All XBRL filings should have corresponding wikidata entry")
    return df, df_1, df_error
end