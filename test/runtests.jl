using ESEF

using DataFrames
using DataFrameMacros
using GeoJSON
using Chain
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
    lei_data = ESEF.get_lei_data(lei)
    ESEF.get_lei_names(lei_data[1]) == ("TYMAN PLC", "LUPUS CAPITAL PLC")
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
    @test names(df) == [
        "_root_",
        "_version_",
        "ae_authorisationNotificationDate",
        "ae_authorisationNotificationDateStr",
        "ae_authorisationWithdrawalEndDateStr",
        "ae_branchAddress",
        "ae_comment",
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
        "ae_lei",
        "ae_micLeiEsmaId",
        "ae_officeType",
        "ae_status",
        "collectorParent",
        "entity_type",
        "id",
        "timestamp",
        "type_s"
    ]
    @test nrow(df) >= 100
    @test nrow(df) <= 200
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
    @test sort(isin_data) == ["DE0007664005", "DE0007664039"]
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
    "CREATE\nLAST\tde\tVOLKSWAGEN AKTIENGESELLSCHAFT\nLAST\tP1278\t529900NNUPAGGOMPXZ31\nLAST\tP17\tQ183\nLAST\tP946\tDE0007664005\nLAST\tP946\tDE0007664039\nLAST\tP31\tQ6881511")
    
    wd_record_2 = ESEF.build_wikidata_record(lei_list)
    @test length(wd_record_2) == 2
    @test (wd_record_2[2] ==
    "CREATE\nLAST\ten\tAPPLE INC.\nLAST\tP1278\tHWUPKR0MPOU8FGXBT394\nLAST\tP17\tQ30\nLAST\tP946\tUS037833DK32\nLAST\tP946\tUS03785C6L74\nLAST\tP946\tUS03785CCS52\nLAST\tP946\tUS03785CGB81\nLAST\tP946\tUS03785CGN20\nLAST\tP946\tUS03785CHW10\nLAST\tP946\tUS03785CLT35\nLAST\tP946\tUS03785CM541\nLAST\tP946\tUS03785CNG95\nLAST\tP946\tUS03785CPQ59\nLAST\tP946\tUS03785CRJ98\nLAST\tP946\tUS03785CRU44\nLAST\tP946\tUS03785CUD81\nLAST\tP946\tUS03785CUT34\nLAST\tP946\tUS03785CYP75\nLAST\tP31\tQ6881511")
end

@testset "Check Quick Statements Routines" begin
    @test ESEF.import_missing_leis_to_wikidata(lei_list) isa Int
    @test ESEF.merge_duplicate_wikidata_on_leis() isa Int
end

@testset "Test patient post (with retries)" begin
    r = ESEF.patient_post("http://httpbin.org/post", [], "{\"a\": 1}")
    @test r["json"] == Dict("a" => 1)
end

@testset "unpack_value_cols" begin
    df = DataFrame(
        a = [missing, Dict("value" => 2)],
        b = [Dict("value" => 3), Dict("value" => 4)],
    )
    df = ESEF.unpack_value_cols(df, [:a, :b])
    @test isequal(df, DataFrame(a = [missing, 2], b = [3, 4]))
end

@testset "get_non_lei_isin_companies_wikidata" begin
    df = ESEF.get_companies_with_isin_without_lei_wikidata()
    names(df) == [ "country"
    "countryLabel"
    "country_alpha_2"
    "entity"
    "entityLabel"
    "isin_value"
    "isin_alpha_2"]
end

@testset "get_facts_for_property" begin
    df = ESEF.get_facts_for_property("P1278")
    @test names(df) == ["object"
    "subject"
    "subjectLabel"
    "predicate"]
    @test nrow(df) > 30000

    df = ESEF.get_full_wikidata_leis()
    @test names(df) == ["object"
    "subject"
    "subjectLabel"
    "predicate"]
    @test nrow(df) > 30000

end

@testset "strip_wikidata_prefix" begin
    df = DataFrame(
        a = [missing, "http://www.wikidata.org/entity/Q2"],
        b = ["http://www.wikidata.org/entity/Q2", "http://www.wikidata.org/entity/Q2"]
    )
    
    @test isequal(ESEF.strip_wikidata_prefix(df, [:a, :b]), DataFrame(
        a = [missing, "Q2"],
        b = ["Q2", "Q2"]
    ))
end

@testset "Search company by name" begin
    @test DataFrame(ESEF.search_company_by_name("Apple")[1, 1:2]) == DataFrame(
        company = ["http://www.wikidata.org/entity/Q312"],
        companyLabel = ["Apple Inc."],
    )
end

@testset "get_esma_regulated_countries" begin
    df = ESEF.get_esma_regulated_countries()
    @test nrow(df) == 29
    @test names(df) == ["esma_countries"]
end

@testset "get_entities_which_are_instance_of_object" begin
    lookup = Dict(:countries => "Q6256")
    df = ESEF.get_entities_which_are_instance_of_object(lookup[:countries])
    @test nrow(df) > 250
    @test nrow(df) < 275
    @test names(df) == ["subject", "subjectLabel", "predicate", "object"]
end

@testset "get_wikidata_economic_and_accounting_concepts" begin
    df = ESEF.get_wikidata_economic_and_accounting_concepts()
    @test nrow(df) > 1000
    @test nrow(df) < 5000
    @test names(df) == ["concept", "conceptLabel"]
end
