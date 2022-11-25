using ESEF

using DataFrames
using GeoJSON
using Test

lei = "529900NNUPAGGOMPXZ31"
lei_list = [lei, "HWUPKR0MPOU8FGXBT394"]

@testset "ESEF.jl Visualizations" begin
    plots = ESEF.generate_esef_homepage_viz()

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
    ESEF.serve_oxigraph()
end

@testset "LEI query" begin
    lei = "213800AAFUV5PKGQU848"
    ESEF.get_lei_names(lei) == ("TYMAN PLC", "LUPUS CAPITAL PLC")
end


@testset "Quick Statement: Merge" begin
    wd_obj = ["Q1", "Q2", "Q3"]
    @test ESEF.compose_merge_statement(wd_obj) == ["MERGE\tQ1\tQ2", "MERGE\tQ1\tQ3"]
end


@testset "ISO Country Lookup" begin
    df = ESEF.get_country_codes()
    @test nrow(df) == 250
    @test ncol(df) == 3
    @test 2 == @chain df @subset((:country == "United Kingdom") | (:country == "Czechoslovakia")) nrow
end

@testset "ESMA Regulated Markets" begin
    df = ESEF.get_regulated_markets_esma()
    @test nrow(df) >= 100
    @test nrow(df) <= 200
    @test ncol(df) == 7
end


@testset "GLEIF LEI API" begin
    lei_data = ESEF.get_lei_data(lei_list)
    @test length(lei_data) == 2
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_data = ESEF.get_lei_data(lei)
    @test length(lei_data) == 1
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_clean = ESEF.extract_lei_information(lei_data[1])
    @test [keys(lei_clean)...] == ["lei", "entity_names", "country", "isins"]
end

@testset "GLEIF ISIN API" begin
    lei = "529900NNUPAGGOMPXZ31"
    isin_data = ESEF.get_isin_data(lei)
    @test isin_data == ["DE0007664005", "DE0007664039"]
end

@testset "Wikidata Mini Analysis" begin
    d_obj = ESEF.esef_wikidata_mini_analysis()
    @test length(d_obj) == 3
    @test names(d_obj[1]) == [
        "key",
        "entity_name",
        "country_alpha_2",
        "date",
        "filing_key",
        "error_count",
        "error_codes",
        "xbrl_json_path",
        "country",
        "region",
        "wikidata_uri",
        "company_label",
        "country_1",
        "country_uri",
        "country_alpha_2_1",
        "isin_id",
        "isin_alpha_2",
        "region_1",
        "isin_country",
        "isin_region",
        "esef_regulated"
    ]
    @test names(d_obj[2]) == ["key", "entity_name", "company_label"]
    @test names(d_obj[3]) == ["key", "error_code"]
end

@testset "truncate_text" begin
    @test ESEF.truncate_text(repeat("a", 100)) == "aaaaaaaaaaaaaaa...aaaaaaaaaaaaaaa"
    @test ESEF.truncate_text(repeat("a", 30)) == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
end

@testset "ESEF Mandate Dataset" begin
    @test names(ESEF.get_esef_mandate_df()) == [
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
    geo = ESEF.generate_esef_basemap()
    @test geo isa GeoJSON.FeatureCollection
end

@testset "ESEF XBRL Filings API" begin
    df, df_error = ESEF.get_esef_xbrl_filings()
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
        "region"
    ]

    @test ncol(df_error) == 2
    @test nrow(df_error) > 1000
    @test names(df_error) == ["key", "error_code"]

    country_rollup = ESEF.calculate_country_rollup(df)

    @test ncol(country_rollup) == 2
    @test nrow(country_rollup) == 26
    @test names(country_rollup) == ["country", "report_count"]
end

@testset "Quick Statement Construction" begin
    @test ESEF.build_quick_statement("LAST", "P31", "Q5") == "LAST\tP31\tQ5"
    @test ESEF.build_quick_statement("LAST", "P31", "Q5") == "LAST\tP31\tQ5"

    @test ESEF.build_quick_statement("LAST", ["P31", "P31"], ["Q5", "Q5"]) == ["LAST\tP31\tQ5", "LAST\tP31\tQ5"]

    @test ESEF.build_quick_statement(["P31", "P31"], ["Q5", "Q5"]) == "CREATE" *
                                                                      "\nLAST\tP31\tQ5" *
                                                                      "\nLAST\tP31\tQ5"
end

@testset "generate_quick_statement_from_lei_obj" begin
    lei_obj = Dict("lei" => "529900NNUPAGGOMPXZ31",
        "entity_names" => [Dict("name" => "VOLKSWAGEN AKTIENGESELLSCHAFT", "language" => "de")],
        "country" => "DE",
        "isins" => ["DE0007664039", "DE0007664005"])

    qs_test_statement = ESEF.generate_quick_statement_from_lei_obj(lei_obj)
    @test (qs_test_statement ==
        "CREATE\nLAST\tde\tVOLKSWAGEN AKTIENGESELLSCHAFT\nLAST\tP1278\t529900NNUPAGGOMPXZ31\nLAST\tP17\tQ183\nLAST\tP946\tDE0007664039\nLAST\tP946\tDE0007664005\nLAST\tP31\tQ6881511")

    wd_record = ESEF.build_wikidata_record(lei)
    @test (wd_record ==
    "CREATE\nLAST\tde\tVOLKSWAGEN AKTIENGESELLSCHAFT\nLAST\tP1278\t529900NNUPAGGOMPXZ31\nLAST\tP17\tQ183\nLAST\tP946\tDE0007664039\nLAST\tP946\tDE0007664005\nLAST\tP31\tQ6881511")
    
    wd_record_2 = ESEF.build_wikidata_record(lei_list)
    @test length(wd_record_2) == 2
    @test wd_record_2[2] == "CREATE\nLAST\ten\tAPPLE INC.\nLAST\tP1278\tHWUPKR0MPOU8FGXBT394\nLAST\tP17\tQ30\nLAST\tP946\tUS03785CBC10\nLAST\tP946\tUS03785CJE93\nLAST\tP946\tUS03785CQ351\nLAST\tP946\tUS03785CMB18\nLAST\tP946\tUS03785CQ682\nLAST\tP946\tUS03785CYG76\nLAST\tP946\tUS03785CMF22\nLAST\tP946\tUS037833AS94\nLAST\tP946\tUS03785CTP31\nLAST\tP946\tUS03785CBH07\nLAST\tP946\tUS03785CN614\nLAST\tP946\tUS03785CJS89\nLAST\tP946\tUS03785C3X40\nLAST\tP946\tUS03785C5B02\nLAST\tP946\tUS03785CA322\nLAST\tP31\tQ6881511" 
end

@testset "Check Quick Statements Routines" begin
    @test ESEF.import_missing_leis_to_wikidata(lei_list) isa Int
    @test ESEF.merge_duplicate_wikidata_on_leis() isa Int
end

@testset "Query Test: get_full_wikidata_leis"
    df = ESEF.get_full_wikidata_leis()
    @test names(df) == ["entity", "entityLabel", "lei_value"]
    @test nrow(df) > 1e5
    @test nrow(df) < 1e7
end

@testset "Test patient post (with retries)" begin
    r = ESEF.patient_post("http://httpbin.org/post", [], "{\"a\": 1}")
    @test r["json"] == Dict("a" => 1)
end

@testset "Test query_sparql function" begin
    api_url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
    sparql_query_file = joinpath(@__DIR__, "..", "queries", "wikidata", "single_lei_lookup.sparql")
    df = query_sparql(api_url, sparql_query_file; params=Dict("lei" => "529900NNUPAGGOMPXZ31"))
    @test names(df) == ["item", "itemLabel"]
    @test nrow(df) == 1

    df = query_wikidata_sparql(sparql_query_file)
    @test names(df) == ["item", "itemLabel"]
    @test nrow(df) == 1
end
