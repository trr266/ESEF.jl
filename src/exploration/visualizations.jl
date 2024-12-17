using AlgebraOfGraphics
using CairoMakie
using Chain
using Colors
using CSV
using DataFrameMacros
using DataFrames
using Dates
using Downloads
using GeoMakie
using GeoMakie
using GeoMakie.GeoJSON
using HTTP
using JSON
using Statistics
using URIParser
using OrderedCollections
using NaturalEarth

trr_266_colors = ["#1b8a8f", "#ffb43b", "#6ecae2", "#944664"] # petrol, yellow, blue, red

function get_esef_mandate_df()
    d_path = joinpath(@__DIR__, "..", "..", "data", "esef_mandate_overview.csv")
    esef_year_df = @chain d_path CSV.read(DataFrame; normalizenames=true)
    return esef_year_df
end

function generate_esef_basemap()
    country_geo = DataFrame(naturalearth("admin_0_countries", 50))
    tiny_country_geo = DataFrame(naturalearth("admin_0_countries", 10))

    mandate_df = get_esef_mandate_df()
    mandate_countries = mandate_df[!, :Country]
    malta = @chain tiny_country_geo @subset(:ADMIN == "Malta")
    europe = @chain country_geo @subset((:ADMIN ∈ mandate_countries) & (:ADMIN != "Malta"))
    return vcat(malta, europe)
end

function generate_esef_report_map(; is_poster=false)
    background_gray = RGBf(0.85, 0.85, 0.85)
    if is_poster
        background_color = :transparent
    else
        background_color = background_gray
    end
    fontsize_theme = Theme(; fontsize=20, backgroundcolor=background_color)
    set_theme!(fontsize_theme)
    dest = "+proj=laea"
    source = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

    fig = Figure(; size=(1000, 500), backgroundcolor=background_color)
    gd = fig[1, 1] = GridLayout()

    ga = GeoAxis(
        gd[1, 1];
        source=source,
        dest=dest,
        title="ESEF Reports Availability by Country",
        subtitle="(XBRL Repository)",
    )
    ga.limits[] = (-28, 35, 35, 72)
    eu_geo = generate_esef_basemap()
    df, df_error = get_esef_xbrl_filings()
    country_rollup = calculate_country_rollup(df)

    eu_geo = leftjoin(eu_geo, country_rollup, on=(:ADMIN => :countryLabel))
    replace!(eu_geo.report_count, missing => 0)

    max_reports = maximum(country_rollup[!, :report_count])
    color_scale_ = range(
        parse(Colorant, "#ffffff"), parse(Colorant, trr_266_colors[2]), max_reports + 1
    )
    for row in eachrow(eu_geo)
        poly!(
            ga,
            row[:geometry];
            strokecolor=RGBf(0.90, 0.90, 0.90),
            strokewidth=1,
            color=row[:report_count],
            colorrange=(0, max_reports),
            colormap=color_scale_,
            label="test",
        )
    end

    cbar = Colorbar(
        gd[1, 2];
        colorrange=(0, max_reports),
        colormap=color_scale_,
        height=Relative(0.65),
        tickformat=(xs -> [x == 600 ? "$(Int(x)) reports" : "$(Int(x))" for x in xs]),
    )

    hidedecorations!(ga)
    # hidespines!(ga)
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

    fig = Figure(; size=(1000, 500))
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

    country_ordered = country_rollup[!, :countryLabel]

    plt = @chain country_rollup begin
        data(_) *
        mapping(
            :countryLabel =>
                renamer((OrderedDict(zip(country_ordered, country_ordered)))...),
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
        @transform(:error_code = truncate_text(:error_code))
        @groupby(:error_code)
        @combine(:error_count = length(:error_code))
        @sort(-:error_count)
        first(15)
    end

    error_ordered = df_error_count[!, :error_code]

    axis = (
        width=800,
        height=300,
        xlabel="Error Count",
        ylabel="Error Type",
        title="ESEF Error Frequency (Top 15)",
        subtitle="(XBRL Repository)",
        xticklabelrotation=-π / 2,
    )

    fg_error_freq_bar = @chain df_error_count begin
        data(_) *
        mapping(
            :error_code => renamer((OrderedDict(zip(error_ordered, error_ordered)))...),
            :error_count,
        ) *
        visual(BarPlot; color=trr_266_colors[1])
    end

    return draw(fg_error_freq_bar; axis=axis)
end

function generate_esef_error_country_heatmap()
    df, df_error = get_esef_xbrl_filings()

    df_error_wide = @chain df_error begin
        leftjoin(df; on=:key)
    end

    df_error_country = @chain df_error_wide begin
        @transform(:error_code = truncate_text(:error_code))
        @groupby(:error_code, :countryLabel)
        @combine(:error_count = length(:error_code))
        @sort(-:error_count)
    end

    axis = (
        width=900,
        height=400,
        ylabel="Country",
        xlabel="Error Code",
        title="Error Frequency by Country and Type",
        xticklabelrotation=π / 2,
    )

    max_errors = maximum(df_error_country[!, :error_count])
    color_scale_ = range(
        parse(Colorant, "#DBDBDB"), parse(Colorant, trr_266_colors[2]), max_errors + 1
    )

    fg_error_country_heatmap = @chain df_error_country begin
        data(_) *
        mapping(:error_code, :countryLabel, :error_count) *
        visual(Heatmap; colormap=color_scale_)
    end

    fig = Figure()
    ag = draw!(fig[1, 1], fg_error_country_heatmap; axis=axis)
    colorbar!(fig[1, 2], ag; label="Error Count")
    resize_to_layout!(fig)

    return fig
end

function generate_esef_publication_date_composite()
    df, df_error = get_esef_xbrl_filings()

    df_country_date = @chain df begin
        @transform(:month = string(floor(Date(:date), Month)))
        @groupby(:month, :countryLabel)
        @combine(:report_count = length(:country))
        @subset(!ismissing(:countryLabel))
        @sort(:report_count)
    end

    fig = Figure()

    axis1 = (
        width=500,
        height=500,
        xlabel="Country",
        ylabel="Date",
        title="Report Publication by Country and Date",
        xticklabelrotation=π / 2,
    )

    max_reports = maximum(df_country_date[!, :report_count])
    color_scale_ = range(
        parse(Colorant, "#DBDBDB"), parse(Colorant, trr_266_colors[2]), max_reports + 1
    )

    fg_country_date = @chain df_country_date begin
        data(_) *
        mapping(:month, :countryLabel, :report_count) *
        visual(Heatmap; colormap=color_scale_)
    end

    ag = draw!(fig[2, 1], fg_country_date; axis=axis1)

    axis2 = (
        width=500,
        height=100,
        xlabel="Date",
        ylabel="Report Count",
        title="Report Publication by Date",
        xticklabelrotation=π / 2,
    )

    fg_date_bar = @chain df_country_date begin
        @groupby(:month)
        @combine(:report_count = sum(:report_count))

        data(_) * mapping(:month, :report_count) * visual(BarPlot; color=trr_266_colors[2])
    end

    draw!(fig[1, 1], fg_date_bar; axis=axis2)

    linkxaxes!(fig.content...)
    hidexdecorations!(fig.content[2])
    colorbar!(fig[2, 2], ag; label="Report Count")
    resize_to_layout!(fig)

    return fig
end

function generate_esef_homepage_viz()
    viz = Dict(
        :esef_country_availability_bar => generate_esef_country_availability_bar(),
        :esef_country_availability_map => generate_esef_report_map(),
        :esef_error_country_heatmap => generate_esef_error_country_heatmap(),
        :esef_error_hist => generate_esef_error_hist(),
        :esef_error_type_freq_bar => generate_esef_error_type_freq_bar(),
        :esef_mandate_overview => generate_esef_mandate_map(),
        :esef_publication_date_composite => generate_esef_publication_date_composite(),
    )

    return viz
end
