using Test
using ESEF

@testset "ESEF.jl" begin
    plots = ESEF.generate_esef_homepage_viz()

    # Check all plots generated
    @test [keys(plots)...] == [
        :esef_country_availability_bar,
        :esef_country_availability_map,
        :esef_error_country_heatmap,
        :esef_error_hist,
        :esef_error_type_freq_bar,
        :esef_errors_followers,
        :esef_mandate_overview,
        :esef_publication_date_composite,
    ]
end
