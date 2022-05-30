using Test
using ESEF

@testset "ESEF.jl" begin
    @test 80 == 80
    plots = ESEF.generate_esef_homepage_viz(; map_output="web")

    # Check all plots generated
    @test [keys(plots)...] == ["esef_error_hist",
        "esef_country_availability_map",
        "esef_error_type_freq_bar",
        "esef_publication_date_composite",
        "esef_errors_followers",
        "esef_mandate_overview",
        "esef_country_availability_bar",
        "esef_country_availability_map_poster",
        "esef_error_country_heatmap"]
end
