using AlgebraOfGraphics
using CairoMakie
using Chain
using Colors
using CSV
using DataFrameMacros
using DataFrames
using Dates
using Downloads
using GeoJSON
using GeoMakie
using GeoMakie
using GeoMakie.GeoJSON
using HTTP
using JSON
using Setfield
using Statistics
using URIParser

trr_266_colors = ["#1b8a8f", "#ffb43b", "#6ecae2", "#944664"] # petrol, yellow, blue, red

function calculate_country_rollup(df)
    country_rollup = @chain df begin
        @subset(!ismissing(:country))
        @groupby(:country)
        @combine(:report_count = length(:country))
        @transform(:report_count = coalesce(:report_count, 0))
        @sort(:report_count; rev=true)
    end
    return country_rollup
end

function get_esef_mandate_df()
    d_path = joinpath(@__DIR__, "..", "data", "esef_mandate_overview.csv")
    esef_year_df = @chain d_path CSV.read(DataFrame; normalizenames=true)
    return esef_year_df
end

function generate_esef_basemap()
    url = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
    country = Downloads.download(url * "ne_50m_admin_0_countries.geojson")
    country_json = JSON.parse(read(country, String))

    tiny_country = Downloads.download(url * "ne_50m_admin_0_tiny_countries.geojson")
    tiny_country_json = JSON.parse(read(tiny_country, String))

    mandate_df = get_esef_mandate_df()

    malta = [
        c for c in tiny_country_json["features"] if c["properties"]["ADMIN"] == "Malta"
    ]
    europe = [
        c for c in country_json["features"] if
        (c["properties"]["ADMIN"] ∈ mandate_df[!, :Country]) &
        (c["properties"]["ADMIN"] != "Malta")
    ]
    country_json["features"] = [malta..., europe...]

    country_geo = GeoJSON.read(JSON.json(country_json))
    return country_geo
end

function generate_esef_report_map()
    background_gray = RGBf(0.85, 0.85, 0.85)
    fontsize_theme = Theme(; fontsize=20, backgroundcolor=background_gray)
    set_theme!(fontsize_theme)
    dest = "+proj=laea"
    source = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

    fig = Figure(; resolution=(1000, 500))
    gd = fig[1, 1] = GridLayout()

    ga = GeoAxis(
        gd[1, 1];
        source=source,
        dest=dest,
        lonlims=(-28, 35),
        latlims=(35, 72),
        title="ESEF Reports Availability by Country",
        subtitle="(XBRL Repository)",
        backgroundcolor=background_gray,
    )

    eu_geojson = generate_esef_basemap()
    df, df_error = get_esef_xbrl_filings()
    country_rollup = calculate_country_rollup(df)

    report_count_vect = map(eu_geojson) do geo
        report_count = (@chain country_rollup @subset(:country == geo.ADMIN) @select(
            :report_count
        ))
        nrow(report_count) > 0 ? report_count[1, 1] : 0
    end

    max_reports = maximum(country_rollup[!, :report_count])
    color_scale_ = range(
        parse(Colorant, "#ffffff"), parse(Colorant, "#ffb43b"), max_reports + 1
    )
    # NOTE: Work around for `ERROR: MethodError: no method matching MultiPolygon(::Point{2, Float32})`
    for (c, report_count) in zip(eu_geojson, report_count_vect)
        poly!(
            ga,
            GeoMakie.geo2basic(c);
            strokecolor=RGBf(0.90, 0.90, 0.90),
            strokewidth=1,
            color=color_scale_[report_count+1],
            label="test",
        )
    end

    cbar = Colorbar(
        gd[1, 2];
        colorrange=(0, max_reports),
        colormap=color_scale_,
        label="ESEF Reports (all-time, per country)",
        height=Relative(0.65),
    )

    hidedecorations!(ga)
    hidespines!(ga)
    colgap!(gd, 1)
    rowgap!(gd, 1)

    cbar.tellheight = true
    cbar.width = 50

    return fig
end

function generate_esef_mandate_map()
    background_gray = RGBf(0.85, 0.85, 0.85)
    fontsize_theme = Theme(; fontsize=20, backgroundcolor=background_gray)
    set_theme!(fontsize_theme)
    dest = "+proj=laea"
    source = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

    fig = Figure(; resolution=(1000, 500))
    gd = fig[1, 1] = GridLayout()

    ga = GeoAxis(
        gd[1, 1];
        source=source,
        dest=dest,
        lonlims=(-28, 35),
        latlims=(35, 72),
        title="ESEF Mandate by Country",
        subtitle="(Based on Issuer's Fiscal Year Start Date)",
        backgroundcolor=background_gray,
    )

    eu_geojson = generate_esef_basemap()

    esef_year_df = get_esef_mandate_df()

    mandate_year_vect = map(eu_geojson) do geo
        mandate_year = (@chain esef_year_df @subset(:Country == geo.ADMIN) @select(
            :Mandate_Affects_Fiscal_Year_Beginning
        ))
        mandate_year[1, 1]
    end

    color_scale_ = parse.((Colorant,), trr_266_colors)
    # NOTE: Work around for `ERROR: MethodError: no method matching MultiPolygon(::Point{2, Float32})`
    for (c, mandate_year) in zip(eu_geojson, mandate_year_vect)
        poly!(
            ga,
            GeoMakie.geo2basic(c);
            strokecolor=RGBf(0.90, 0.90, 0.90),
            strokewidth=1,
            color=color_scale_[mandate_year - 2019],
            label=string(mandate_year),
        )
    end

    axislegend(ga; merge=true)

    hidedecorations!(ga)
    hidespines!(ga)
    colgap!(gd, 1)
    rowgap!(gd, 1)

    return fig
end

function generate_esef_error_hist()
    df, df_error = get_esef_xbrl_filings()

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
    
    return draw(plt; axis)
end

function generate_esef_errors_followers()
    # TODO: figure out why entries are not unique...
    df_wikidata_lei = get_lei_companies_wikidata()
    df_wikidata_lei = enrich_wikidata_with_twitter_data(df_wikidata_lei)

    # TODO: backfill twitter profiles for xbrl entries?
    df, df_error = get_esef_xbrl_filings()


    df = @chain df begin
        leftjoin(
            df_wikidata_lei; on=(:key => :lei_id), matchmissing=:notequal, makeunique=true
        )
    end
    
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

   return draw(plt; axis)
end

function generate_esef_country_availability_bar()
    df, df_error = get_esef_xbrl_filings()

    country_rollup = calculate_country_rollup(df)

    axis = (
        width=500,
        height=250,
        xlabel="",
        ylabel="Report Count",
        title="ESEF Report Availability by Country",
        subtitle="(XBRL Repository)",
        xticklabelrotation=pi / 2,
    )

    country_ordered = country_rollup[!, :country]

    plt = @chain country_rollup begin
        data(_) *
        mapping(
            :country => renamer((OrderedDict(zip(country_ordered, country_ordered)))...),
            :report_count,
        ) *
        visual(BarPlot; color=trr_266_colors[1])
    end

    return draw(plt; axis)
end

function generate_esef_error_type_freq_bar()
    df, df_error = get_esef_xbrl_filings()

    df_error_wide = @chain df_error begin
        leftjoin(df; on=:key)
    end

    df_error_count = @chain df_error_wide begin
        @groupby(:error_code)
        @combine(:error_count = length(:error_code))
        @sort(:error_count)
    end

    error_ordered = df_error_count[!, :error_code]

    axis = (
        width=500,
        height=250,
        xlabel="Error Count",
        ylabel="Error Count",
        title="ESEF Error Frequency",
        subtitle="(XBRL Repository)",
    )

    fg_error_freq_bar = @chain df_error_count begin
        data(_) *
        mapping(
            :error_code => renamer((OrderedDict(zip(error_ordered, error_ordered)))...),
            :error_count,
        ) *
        visual(BarPlot; color=trr_266_colors[1])
    end

    return fg_error_freq_bar
end

function generate_esef_error_country_heatmap()
    df, df_error = get_esef_xbrl_filings()

    df_error_wide = @chain df_error begin
        leftjoin(df; on=:key)
    end

    df_error_country = @chain df_error_wide begin
        @groupby(:error_code, :country)
        @combine(:error_count = length(:error_code))
    end

    axis = (
        width=500,
        height=250,
        xlabel="Country",
        ylabel="Error Code",
        title="Error Frequency by Country and Type",
    )

    fg_error_country_heatmap = @chain df_error_country begin
        data(_) *
        mapping(
            :country,
            :error_code,
            color = :error_count
        ) *
        visual(Heatmap; color=trr_266_colors[2])
    end
    
    return fg_error_country_heatmap
end

function generate_esef_publication_date_composite()
    df, df_error = get_esef_xbrl_filings()

    df_country_date = @chain df begin
        @groupby(:date, :country)
        @combine(:report_count = length(:country))
        @sort(:report_count)
    end

    fig = Figure()

    axis = (
        width=500,
        height=500,
        xlabel="Country",
        ylabel="Date",
        title="Report Publication by Country and Date",
    )

    fg_country_date = @chain df_country_date begin
        data(_) *
        mapping(
            :country,
            :date,
            color = :report_count
        ) *
        visual(Heatmap; color=trr_266_colors[2])
    end
    draw!(fig[1, 2], fg_country_date)


    axis = (
        width=500,
        height=100,
        xlabel="Date",
        ylabel="Report Count",
        title="Report Publication by Date",
    )

    fg_date_bar = @chain df_country_date begin
        @groupby(:country)
        @transform(:report_count = sum(:report_count))
        data(_) *
        mapping(
            :date,
            :report_count,
        ) *
        visual(BarPlot; color=trr_266_colors[2])
    end
    draw!(fig[1, 1], fg_date_bar)
end

function generate_esef_homepage_viz(; map_output="web")
    viz = Dict(
        "esef_error_hist" => generate_esef_error_hist(),
        "esef_country_availability_map" => generate_esef_report_map(),
        "esef_error_type_freq_bar" => generate_esef_error_type_freq_bar(),
        "esef_publication_date_composite" => generate_esef_publication_date_composite(),
        "esef_errors_followers" => generate_esef_errors_followers(),
        "esef_mandate_overview" => generate_esef_mandate_map(),
        "esef_country_availability_bar" => generate_esef_country_availability_bar(),
        "esef_error_country_heatmap" => generate_esef_error_country_heatmap(),
    )

    return viz
end
