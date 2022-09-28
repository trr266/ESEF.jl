using CairoMakie, GeoMakie
using GeoMakie.GeoJSON
using GeometryBasics
using Downloads
using JSON
using Chain
using DataFrameMacros
using ESEF
using DataFrames

df, df_error = ESEF.get_esef_xbrl_filings()

country_rollup = @chain df begin
    @groupby(:country)
    @combine(:report_count = length(:country))
    @transform(:report_count = coalesce(:report_count, 0))
    @subset(!ismissing(:country))
end


function generate_esef_basemap(country_rollup)
    url = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
    country = Downloads.download(url * "ne_50m_admin_0_countries.geojson")
    country_json = JSON.parse(read(country, String))

    tiny_country = Downloads.download(url * "ne_50m_admin_0_tiny_countries.geojson")
    tiny_country_json = JSON.parse(read(tiny_country, String))

    malta = [c for c in tiny_country_json["features"] if c["properties"]["ADMIN"] == "Malta"]
    europe = [c for c in country_json["features"] if (c["properties"]["ADMIN"] ∈ country_rollup[!, :country]) & (c["properties"]["ADMIN"] != "Malta")]
    country_json["features"] = [malta..., europe...]

    country_geo = GeoJSON.read(JSON.json(country_json))
    return country_geo
end



function generate_esef_report_map()
    background_gray = RGBf(0.85, 0.85, 0.85)
    fontsize_theme = Theme(fontsize = 20,  backgroundcolor = background_gray)
    set_theme!(fontsize_theme)
    dest = "+proj=laea"
    source = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

    fig = Figure(resolution = (1000,500))
    gd = fig[1, 1] = GridLayout()

    ga = GeoAxis(
        gd[1, 1];
        source = source,
        dest = dest,
        lonlims=(-28, 35),
        latlims = (35, 72),
        title="ESEF Reports Availability by Country",
        subtitle = "(XBRL Repository)",
        backgroundcolor = background_gray,
        )

    eu_geojson = generate_esef_basemap(country_rollup)

    report_count_vect = map(eu_geojson) do geo
        report_count = (@chain country_rollup @subset(:country == geo.ADMIN) @select(:report_count))
        nrow(report_count) > 0 ? report_count[1, 1] : missing
    end

    max_reports = maximum(country_rollup[!, :report_count])
    color_scale_ = range(parse(Colorant, "#ffffff"), parse(Colorant, "#ffb43b"), max_reports+1)
    # NOTE: Work around for `ERROR: MethodError: no method matching MultiPolygon(::Point{2, Float32})`
    for (c, report_count) in zip(eu_geojson, report_count_vect)
        poly!(ga, GeoMakie.geo2basic(c);
            strokecolor = RGBf(0.90, 0.90, 0.90),
            strokewidth = 1,
            color= color_scale_[report_count],
            label="test"
        )
    end


    cbar = Colorbar(gd[1,2]; colorrange = (0, max_reports), colormap = color_scale_, label = "ESEF Reports (all-time, per country)", height = Relative(0.65))

    hidedecorations!(ga)
    hidespines!(ga)
    colgap!(gd, 1)
    rowgap!(gd, 1)

    cbar.tellheight = true
    cbar.width = 50

    return fig
end


