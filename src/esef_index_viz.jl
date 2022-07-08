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
using VegaDatasets
using VegaLite
using Setfield

trr_266_colors = ["#1b8a8f", "#ffb43b", "#6ecae2", "#944664"] # petrol, yellow, blue, red

function generate_esef_homepage_viz(; map_output="web")
    # TODO: figure out why entries are not unique...
    df_wikidata_lei = get_lei_companies_wikidata()
    df_wikidata_lei = enrich_wikidata_with_twitter_data(df_wikidata_lei)

    # TODO: backfill twitter profiles for xbrl entries?
    df, df_error = get_esef_xbrl_filings()

    viz = Dict()

    df = @chain df begin
        leftjoin(
            df_wikidata_lei; on=(:key => :lei_id), matchmissing=:notequal, makeunique=true
        )
    end

    pct_error_free = @chain df begin
        @transform(:error_free_report = :error_count == 0)
        @combine(:error_free_report_pct = round(mean(:error_free_report) * 100; digits=0))
        _[1, :error_free_report_pct]
    end

    axis = (
        width=500,
        height=250,
        xticks=[1, 50:50:500...],
        xlabel="Error Count",
        ylabel="Filing Count",
        title="Errored ESEF Filings by Error Count ($(pct_error_free)% error free)",
    )
    plt = @chain df begin
        @subset(:error_count != 0)
        data(_) *
        mapping(:error_count) *
        histogram(; bins=range(1, 500; length=50)) *
        visual(; color=trr_266_colors[1])
    end

    fg1 = draw(plt; axis)
    viz["esef_error_hist"] = fg1

    axis = (
        width=500,
        height=500,
        xticks=[1, 50:50:500...],
        ylabel="Log1p Error Count",
        xlabel="Log1p Twitter Follower Count (Cumulative)",
        title="ESEF Filing Errors by Twitter Follower Count",
    )

    plt = @chain df begin
        @subset(!ismissing(:agg_followers_count))
        @transform(
            :error_count_log = log1p(:error_count),
            :agg_followers_count_log = log1p(:agg_followers_count)
        )
        data(_) *
        mapping(:agg_followers_count_log, :error_count_log) *
        (linear() + visual(Scatter; color=trr_266_colors[1]))
    end

    fg1 = draw(plt; axis)
    viz["esef_errors_followers"] = fg1

    world110m = dataset("world-110m")

    world_geojson = @chain "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json" URI()

    country_rollup = @chain df begin
        @groupby(:country)
        @combine(:report_count = length(:country))
        @transform(:report_count = coalesce(:report_count, 0))
    end

    # jscpd:ignore-start

    # First is for web, second for poster
    map_heights = [("web", 300), ("poster", 270)]

    for map_height in map_heights
        map_output = map_height[1]
        map_height = map_height[2]
        fg2a = @vlplot(
            width = 500,
            height = map_height,
            title = {
                text = "ESEF Report Availability by Country", subtitle = "(XBRL Repository)"
            }
        )

        fg2b = @vlplot(
            width = 500,
            height = map_height,
            mark = {:geoshape, stroke = :white, fill = :lightgray},
            data = {url = world_geojson, format = {type = :topojson, feature = :countries}},
            projection = {type = :azimuthalEqualArea, scale = 525, center = [15, 53]},
        )

        fg2c = @vlplot(
            width = 500,
            height = map_height,
            mark = {:geoshape, stroke = :white},
            data = {url = world_geojson, format = {type = :topojson, feature = :countries}},
            transform = [{
                lookup = "properties.name",
                from = {
                    data = (@chain country_rollup @subset(:report_count > 0)),
                    key = :country,
                    fields = ["report_count"],
                },
            }],
            projection = {type = :azimuthalEqualArea, scale = 525, center = [15, 53]},
            fill = {
                "report_count:q",
                axis = {title = "Report Count"},
                scale = {range = ["#ffffff", trr_266_colors[2]]},
            },
        )

        fg2 = (fg2a + fg2b + fg2c)

        if map_output == "web"
            viz["esef_country_availability_map"] = fg2
        end

        # Make tweaks for poster
        if map_output == "poster"
            # Make tweaks for poster
            fg2 = @set fg2.background = nothing # transparent background
            fg2 = @set fg2.config = ("view" => ("stroke" => "transparent")) # remove grey border
            fg2 = @set fg2.layer[2]["encoding"]["fill"]["legend"] = nothing # drop legend
            fg2 = @set fg2.title = nothing

            viz["esef_country_availability_map_poster"] = fg2
        end
    end

    fg2_bar = @vlplot(
        {:bar, color = trr_266_colors[1]},
        width = 500,
        height = 300,
        x = {"country:o", title = nothing, sort = "-y"},
        y = {:report_count, title = "Report Count"},
        title = {
            text = "ESEF Report Availability by Country", subtitle = "(XBRL Repository)"
        },
    )((@chain country_rollup @subset(:report_count > 0)))
    viz["esef_country_availability_bar"] = fg2_bar

    d_path = joinpath(@__DIR__, "..", "data", "esef_mandate_overview.csv")
    esef_year_df = @chain d_path CSV.read(DataFrame; normalizenames=true)

    fg3a = @vlplot(
        width = 500,
        height = 300,
        title = {
            text = "ESEF Mandate by Country",
            subtitle = "(Based on Issuer's Fiscal Year Start Date)",
        }
    )

    fg3b = @vlplot(
        mark = {:geoshape, stroke = :white, fill = :lightgray},
        data = {url = world_geojson, format = {type = :topojson, feature = :countries}},
        projection = {type = :azimuthalEqualArea, scale = 525, center = [15, 53]},
    )

    fg3c = @vlplot(
        mark = {:geoshape, stroke = :white},
        width = 500,
        height = 300,
        data = {url = world_geojson, format = {type = :topojson, feature = :countries}},
        transform = [
            {
                lookup = "properties.name",
                from = {
                    data = esef_year_df,
                    key = :Country,
                    fields = ["Mandate_Affects_Fiscal_Year_Beginning"],
                },
            },
            {filter = "isValid(datum.Mandate_Affects_Fiscal_Year_Beginning)"},
        ],
        projection = {type = :azimuthalEqualArea, scale = 525, center = [15, 53]},
        color = {
            "Mandate_Affects_Fiscal_Year_Beginning:O",
            axis = {title = "Mandate Starts"},
            scale = {range = trr_266_colors},
        },
    )

    fg3 = (fg3a + fg3b + fg3c)
    viz["esef_mandate_overview"] = fg3

    # jscpd:ignore-end

    df_error_wide = @chain df_error begin
        leftjoin(df; on=:key)
    end

    df_error_count = @chain df_error_wide begin
        @groupby(:error_code)
        @combine(:error_count = length(:error_code))
    end

    fg_error_freq_bar = @vlplot(
        {:bar, color = trr_266_colors[1]},
        width = 500,
        height = 500,
        y = {"error_code:o", title = "Error Code", sort = "-x"},
        x = {"error_count", title = "Error Count"},
        title = {text = "ESEF Error Frequency", subtitle = "(XBRL Repository)"}
    )(df_error_count)
    viz["esef_error_type_freq_bar"] = fg_error_freq_bar

    df_error_country = @chain df_error_wide begin
        @groupby(:error_code, :country)
        @combine(:error_count = length(:error_code))
    end

    fg_error_country_heatmap = @vlplot(
        :rect,
        width = 500,
        height = 500,
        x = {"country:o", title = nothing},
        y = {"error_code:o", title = "Error Code"},
        color = {
            :error_count,
            title = "Error Count",
            scale = {range = ["#ffffff", trr_266_colors[2]]},
        },
        title = "Error Frequency by Country and Type"
    )(df_error_country)
    viz["esef_error_country_heatmap"] = fg_error_country_heatmap

    df_country_date = @chain df begin
        @groupby(:date, :country)
        @combine(:report_count = length(:country))
    end

    fg_country_date = @vlplot(
        :rect,
        width = 500,
        height = 500,
        y = {"country:o", title = nothing},
        x = {"date:o", title = "Date"},
        color = {
            "report_count:q",
            title = "Report Count",
            scale = {range = ["#ffffff", trr_266_colors[2]]},
        },
        title = "Report Publication by Country and Date"
    )(df_country_date)

    fg_date_bar = @vlplot(
        {:bar, color = trr_266_colors[2]},
        width = 500,
        height = 100,
        y = {"sum(report_count)", title = "Report Count"},
        x = {"date:o", title = "Date"},
        title = "Report Publication by Date"
    )(df_country_date)

    fg_date_composite = [fg_date_bar; fg_country_date]
    viz["esef_publication_date_composite"] = fg_date_composite

    return viz
end
