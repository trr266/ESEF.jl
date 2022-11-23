using ESEF

using DataFrames
using GeoJSON
using Test

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
    lei = "529900NNUPAGGOMPXZ31"
    lei_data = ESEF.get_lei_data([lei, "HWUPKR0MPOU8FGXBT394"])
    @test length(lei_data) == 2
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_data = ESEF.get_lei_data(lei)
    @test length(lei_data) == 1
    @test [keys(lei_data[1])...] == ["links", "attributes", "id", "type", "relationships"]

    lei_clean_data = ESEF.extract_lei_information(lei_data[1])
    @test [keys(lei_clean_data)...] == ["name", "lei", "country", "isins"]
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


# lei = "529900NNUPAGGOMPXZ31"
# lei_data = ESEF.get_lei_data(lei)
# ESEF.extract_lei_information(lei_data[1])

# lei_data = ESEF.get_lei_data([lei, "HWUPKR0MPOU8FGXBT394"])


# using Chain
# using DataFrameMacros
# using DataFrames
# # function m1(a, b)
# #     join(join(a, ","), join(b, ","), "\n")
# # end
# # @chain DataFrame(a = [1,2,3]; b = [1,2,3]) @combine(@bycol m1(:a, :b))

