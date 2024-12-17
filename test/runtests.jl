using ESEF:
    build_quick_statement,
    build_wikidata_record,
    build_xbrl_dataframe,
    calculate_country_rollup,
    compose_merge_statement,
    export_concept_count_table,
    export_profit_table,
    extract_lei_information,
    generate_esef_basemap,
    generate_esef_homepage_viz,
    generate_quick_statement_from_lei_obj,
    get_accounting_facts,
    get_companies_with_isin_without_lei_wikidata,
    get_entities_which_are_instance_of_object,
    get_esef_mandate_df,
    get_esef_xbrl_filings,
    get_esma_regulated_countries,
    get_facts_for_property,
    get_full_wikidata_leis,
    get_isin_data,
    get_lei_data,
    get_lei_names,
    get_regulated_markets_esma,
    get_wikidata_country_iso2_lookup,
    get_wikidata_economic_and_accounting_concepts,
    import_missing_leis_to_wikidata,
    merge_duplicate_wikidata_on_leis,
    patient_post,
    process_xbrl_filings,
    rehydrate_uri_entity,
    search_company_by_name,
    serve_esef_data,
    serve_oxigraph,
    strip_wikidata_prefix,
    truncate_text,
    unpack_value_cols,
    query_local_db_sparql,
    export_equity_table,
    export_total_assets_table

using DataFrames
using DataFrameMacros
using GeoJSON
using Chain
using Test

lei = "529900NNUPAGGOMPXZ31"
lei_list = [lei, "HWUPKR0MPOU8FGXBT394"]

@testset "ESEF.jl Visualizations" begin
    plots = generate_esef_homepage_viz()

    # Check all plots generated
    @test sort([keys(plots)...]) == sort([
        :esef_country_availability_bar,
        :esef_country_availability_map,
        :esef_error_country_heatmap,
        :esef_error_hist,
        :esef_error_type_freq_bar,
        :esef_mandate_overview,
        :esef_publication_date_composite,
    ])
end

@testset "oxigraph db load" begin
    serve_oxigraph(rebuild_db = true)
end

@testset "esef db test load" begin
    serve_esef_data(test = true)
end

@testset "wikidata helper" begin
    rehydrate_uri_entity("http://example.org/ifrs-full%3AAdjustmentsForIncomeTaxExpense") ==
    "ifrs-full:AdjustmentsForIncomeTaxExpense"
end

@testset "export_concept_count_table, export_profit_table, export_equity_table, export_total_assets_table" begin
    process, port = serve_esef_data(test = true, keep_open = true)

    q_path = joinpath(@__DIR__, "..", "queries", "local", "local_query_test.sparql")
    df = query_local_db_sparql(q_path, port)
    d_ = df[!, :count][1]["value"]
    @test names(df) == ["count"]
    @test nrow(df) == 1
    @test parse(Int, d_) > 20000 & parse(Int, d_) < 100000

    df = export_concept_count_table(port)
    @test names(df) == ["concept", "frequency"]
    @test nrow(df) > 100 & nrow(df) < 500

    df = export_profit_table(port)
    @test names(df) == ["entity", "period", "unit", "decimals", "value"]
    @test nrow(df) > 50 & nrow(df) < 200


    df = export_equity_table(port)
    @test names(df) == ["entity", "period", "unit", "decimals", "value"]
    @test nrow(df) > 50 & nrow(df) < 2000


    df = export_total_assets_table(port)
    @test names(df) == ["entity", "period", "unit", "decimals", "value"]
    @test nrow(df) > 2 & nrow(df) < 2000

    kill(process)
end

@testset "export_concept_count_table, export_equity_table" begin
    process, port = serve_esef_data(test = true, keep_open = true)

    q_path = joinpath(@__DIR__, "..", "queries", "local", "local_query_test.sparql")
    df = query_local_db_sparql(q_path, port)
    d_ = df[!, :count][1]["value"]
    @test names(df) == ["count"]
    @test nrow(df) == 1
    @test parse(Int, d_) > 20000 & parse(Int, d_) < 100000

    df = export_concept_count_table(port)
    @test names(df) == ["concept", "frequency"]
    @test nrow(df) > 100 & nrow(df) < 500

    df = export_equity_table(port)
    @test names(df) == ["entity", "period", "unit", "decimals", "value"]
    @test nrow(df) > 50 & nrow(df) < 200

    kill(process)
end

@testset "export_concept_count_table, export_total_assets_table" begin
    process, port = serve_esef_data(test = true, keep_open = true)

    q_path = joinpath(@__DIR__, "..", "queries", "local", "local_query_test.sparql")
    df = query_local_db_sparql(q_path, port)
    d_ = df[!, :count][1]["value"]
    @test names(df) == ["count"]
    @test nrow(df) == 1
    @test parse(Int, d_) > 20000 & parse(Int, d_) < 100000

    df = export_concept_count_table(port)
    @test names(df) == ["concept", "frequency"]
    @test nrow(df) > 100 & nrow(df) < 500

    df = export_total_assets_table(port)
    @test names(df) == ["entity", "period", "unit", "decimals", "value"]
    @test nrow(df) > 50 & nrow(df) < 200

    kill(process)
end

@testset "LEI query" begin
    lei = "213800AAFUV5PKGQU848"
    lei_data = get_lei_data(lei)
    get_lei_names(lei_data[1]) == ("TYMAN PLC", "LUPUS CAPITAL PLC")
end


@testset "Quick Statement: Merge" begin
    wd_obj = ["Q1", "Q2", "Q3"]
    @test compose_merge_statement(wd_obj) == ["MERGE\tQ1\tQ2", "MERGE\tQ1\tQ3"]
end

@testset "ESMA Regulated Markets" begin
    df = get_regulated_markets_esma()
    @test names(df) == [
        "_root_",
        "_version_",
        "ae_authorisationNotificationDate",
        "ae_authorisationNotificationDateStr",
        "ae_authorisationWithdrawalEndDateStr",
        "ae_branchAddress",
        "ae_comment",
        "ae_commercialName",
        "ae_competentAuthority",
        "ae_dbId",
        "ae_entityName",
        "ae_entityTypeCode",
        "ae_entityTypeLabel",
        "ae_headOfficeAddress",
        "ae_headOfficeLei",
        "ae_homeMemberState",
        "ae_hostMemberState",
        "ae_lastUpdate",
        "ae_lastUpdateStr",
        "ae_legalform",
        "ae_lei",
        "ae_micLeiEsmaId",
        "ae_officeType",
        "ae_status",
        "ae_website",
        "collectorParent",
        "entity_type",
        "id",
        "timestamp",
        "type_s",
    ]
    @test nrow(df) >= 100
    @test nrow(df) <= 200
end


@testset "GLEIF LEI API" begin
    lei_data = get_lei_data(lei_list)
    @test length(lei_data) == 2
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_data = get_lei_data(lei)
    @test length(lei_data) == 1
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_clean = extract_lei_information(lei_data[1])
    @test [keys(lei_clean)...] == ["lei", "entity_names", "country", "isins"]
end

@testset "GLEIF ISIN API" begin
    lei = "529900NNUPAGGOMPXZ31"
    isin_data = get_isin_data(lei)
    @test sort(isin_data) == ["DE0007664005", "DE0007664039"]
end

@testset "truncate_text" begin
    @test truncate_text(repeat("a", 100)) == "aaaaaaaaaaaaaaa...aaaaaaaaaaaaaaa"
    @test truncate_text(repeat("a", 30)) == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
end

@testset "ESEF Mandate Dataset" begin
    @test names(get_esef_mandate_df()) == [
        "Country",
        "XBRL_Repo",
        "Mandate_Affects_Fiscal_Year_Beginning",
        "API_Access_to_Reports",
        "Notes",
        "Column6",
        "Column7",
    ]
end

@testset "ESEF Visualizations: European Basemap" begin
    geo = generate_esef_basemap()
    @test geo isa GeoJSON.FeatureCollection
end

@testset "ESEF XBRL Filings API" begin
    df, df_error = get_esef_xbrl_filings()
    @test ncol(df) == 10
    @test nrow(df) > 4000
    @test names(df) == [
        "key",
        "entity_name",
        "country_alpha_2",
        "date",
        "filing_key",
        "error_count",
        "error_codes",
        "xbrl_json_path",
        "country",
        "countryLabel",
    ]

    @test ncol(df_error) == 2
    @test nrow(df_error) > 1000
    @test names(df_error) == ["key", "error_code"]

    country_rollup = calculate_country_rollup(df)

    @test ncol(country_rollup) == 2
    @test nrow(country_rollup) == 29
    @test names(country_rollup) == ["countryLabel", "report_count"]
end

@testset "Quick Statement Construction" begin
    @test build_quick_statement("LAST", "P31", "Q5") == "LAST\tP31\tQ5"
    @test build_quick_statement("LAST", "P31", "Q5") == "LAST\tP31\tQ5"

    @test build_quick_statement("LAST", ["P31", "P31"], ["Q5", "Q5"]) ==
          ["LAST\tP31\tQ5", "LAST\tP31\tQ5"]

    @test build_quick_statement(["P31", "P31"], ["Q5", "Q5"]) ==
          "CREATE" * "\nLAST\tP31\tQ5" * "\nLAST\tP31\tQ5"
end

@testset "generate_quick_statement_from_lei_obj" begin
    lei_obj = Dict(
        "lei" => "529900NNUPAGGOMPXZ31",
        "entity_names" =>
            [Dict("name" => "VOLKSWAGEN AKTIENGESELLSCHAFT", "language" => "de")],
        "country" => "DE",
        "isins" => ["DE0007664039", "DE0007664005"],
    )

    qs_test_statement = generate_quick_statement_from_lei_obj(lei_obj)
    @test (
        qs_test_statement ==
        "CREATE\nLAST\tde\tVOLKSWAGEN AKTIENGESELLSCHAFT\nLAST\tP1278\t529900NNUPAGGOMPXZ31\nLAST\tP17\tQ183\nLAST\tP946\tDE0007664039\nLAST\tP946\tDE0007664005\nLAST\tP31\tQ6881511"
    )

    wd_record = build_wikidata_record(lei)
    @test (
        wd_record ==
        "CREATE\nLAST\tde\tVOLKSWAGEN AKTIENGESELLSCHAFT\nLAST\tP1278\t529900NNUPAGGOMPXZ31\nLAST\tP17\tQ183\nLAST\tP946\tDE0007664005\nLAST\tP946\tDE0007664039\nLAST\tP31\tQ6881511"
    )

    wd_record_2 = build_wikidata_record(lei_list)
    @test length(wd_record_2) == 2
    @test occursin(
        "CREATE\nLAST\ten\tAPPLE INC.\nLAST\tP1278\tHWUPKR0MPOU8FGXBT394",
        wd_record_2[2],
    )

end

@testset "Check Quick Statements Routines" begin
    @test import_missing_leis_to_wikidata(lei_list) isa Int
    @test merge_duplicate_wikidata_on_leis() isa Int
end

@testset "Test patient post (with retries)" begin
    r = patient_post("http://httpbin.org/post", [], "{\"a\": 1}"; n_retries = 5)
    @test r["json"] == Dict("a" => 1)
end

@testset "unpack_value_cols" begin
    df = DataFrame(
        a = [missing, Dict("value" => 2)],
        b = [Dict("value" => 3), Dict("value" => 4)],
    )
    df = unpack_value_cols(df, [:a, :b])
    @test isequal(df, DataFrame(a = [missing, 2], b = [3, 4]))
end

@testset "get_non_lei_isin_companies_wikidata" begin
    df = get_companies_with_isin_without_lei_wikidata()
    names(df) == [
        "country"
        "countryLabel"
        "country_alpha_2"
        "entity"
        "entityLabel"
        "isin_value"
        "isin_alpha_2"
    ]
end

@testset "get_facts_for_property" begin
    wikidata_rdf_export_cols = [
        "object"
        "subject"
        "subjectLabel"
        "predicate"
    ]

    df = get_facts_for_property("P1278")
    @test names(df) == wikidata_rdf_export_cols
    @test nrow(df) > 30000

    df = get_full_wikidata_leis()
    @test names(df) == wikidata_rdf_export_cols
    @test nrow(df) > 30000


    df = get_full_wikidata_leis()
    @test names(df) == wikidata_rdf_export_cols

    df = get_accounting_facts()
    @test names(df) == wikidata_rdf_export_cols
end

@testset "strip_wikidata_prefix" begin
    df = DataFrame(
        a = [missing, "http://www.wikidata.org/entity/Q2"],
        b = ["http://www.wikidata.org/entity/Q2", "http://www.wikidata.org/entity/Q2"],
    )

    @test isequal(
        strip_wikidata_prefix(df, [:a, :b]),
        DataFrame(a = [missing, "Q2"], b = ["Q2", "Q2"]),
    )
end

@testset "Search company by name" begin
    @test DataFrame(search_company_by_name("Apple")[1, 1:2]) == DataFrame(
        company = ["http://www.wikidata.org/entity/Q312"],
        companyLabel = ["Apple Inc."],
    )
end

@testset "get_esma_regulated_countries" begin
    df = get_esma_regulated_countries()
    @test nrow(df) == 29
    @test names(df) == ["esma_countries"]
end

@testset "get_entities_which_are_instance_of_object" begin
    lookup = Dict(:countries => "Q6256")
    df = get_entities_which_are_instance_of_object(lookup[:countries])
    @test nrow(df) > 250
    @test nrow(df) < 275
    @test names(df) == ["subject", "subjectLabel", "predicate", "object"]
end

@testset "get_wikidata_economic_and_accounting_concepts" begin
    df = get_wikidata_economic_and_accounting_concepts()
    @test nrow(df) > 1000
    @test nrow(df) < 5000
    @test names(df) == ["concept", "conceptLabel"]
end

@testset "get_wikidata_country_iso2_lookup" begin
    df = get_wikidata_country_iso2_lookup()
    @test nrow(df) > 250
    @test nrow(df) < 275
    @test names(df) == ["country", "countryLabel", "country_alpha_2"]
end

@testset "Build RDF Dataframes" begin
    df = build_xbrl_dataframe(; test = true)
    @test names(df) == ["subject", "predicate", "object", "rdf_line"]
end

@testset "process_xbrl_filings" begin
    out_dir = ".cache"
    rm(out_dir; force = true, recursive = true)
    process_xbrl_filings(out_dir = out_dir, test = true)

    files_ = [".cache/concept_df.arrow", ".cache/profit_df.arrow"]
    for f in files_
        @test isfile(f)
    end

    rm(out_dir; force = true, recursive = true)
end
