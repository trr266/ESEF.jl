using Test
using ESEF

@testset "ESEF.jl Visualizations" begin
    plots = ESEF.generate_esef_homepage_viz()

    # Check all plots generated
    @test sort([keys(plots)...]) == sort([
        :esef_country_availability_bar,
        :esef_country_availability_map,
        :esef_error_country_heatmap,
        :esef_error_hist,
        :esef_error_type_freq_bar,
        :esef_errors_followers,
        :esef_mandate_overview,
        :esef_publication_date_composite,
    ])
end

@testset "oxigraph db load" begin
    ESEF.serve_oxigraph()
end

@testset "LEI query" begin
    ESEF.get_lei_names("213800AAFUV5PKGQU848") == ("TYMAN PLC", "LUPUS CAPITAL PLC")
end


@testset "Quick Statement: Merge" begin
    @test ESEF.compose_merge_statement(["Q1", "Q2", "Q3"]) == ["MERGE\tQ1\tQ2", "MERGE\tQ1\tQ3"]
end


@testset "ISO Country Lookup" begin
    df = ESEF.get_country_codes()
    @test nrow(df) == 250
    @test ncol(df) == 3
    @test 2 == @chain df @subset((:country == "United Kingdom") | (:country == "Czechoslovakia")) nrow
end

@testset "ESMA Regulated Markets" begin
    df = get_regulated_markets_esma()
    @test nrow(df) >= 100
    @test nrow(df) <= 200
    @test ncol(df) == 7
end


@testset "GLEIF LEI API" begin
    lei_obj = get_lei_data("549300PSVDV3P50KHS39")
    @test [keys(lei_obj)...] == ["meta", "links", "data"]
end
