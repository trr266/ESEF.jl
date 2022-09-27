using CairoMakie, GeoMakie
using GeoMakie.GeoJSON
using GeometryBasics
using Downloads
using JSON
using Chain
using DataFrameMacros
using ESEF
using DataFrames

df_wikidata_lei = ESEF.get_lei_companies_wikidata()
df_wikidata_lei = ESEF.enrich_wikidata_with_twitter_data(df_wikidata_lei)

# TODO: backfill twitter profiles for xbrl entries?
df, df_error = ESEF.get_esef_xbrl_filings()

df = @chain df begin
    leftjoin(
        df_wikidata_lei; on=(:key => :lei_id), matchmissing=:notequal, makeunique=true
    )
end

country_rollup = @chain df begin
    @groupby(:country)
    @combine(:report_count = length(:country))
    @transform(:report_count = coalesce(:report_count, 0))
    @subset(!ismissing(:country))
end


function generate_esef_basemap(country_rollup)
    url = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
    country = Downloads.download(url * "ne_110m_admin_0_countries.geojson")
    country_json = JSON.parse(read(country, String))

    tiny_country = Downloads.download(url * "ne_110m_admin_0_tiny_countries.geojson")
    tiny_country_json = JSON.parse(read(tiny_country, String))

    malta = [c for c in tiny_country_json["features"] if c["properties"]["ADMIN"] == "Malta"]
    europe = [c for c in country_json["features"] if (c["properties"]["ADMIN"] ∈ country_rollup[!, :country]) & (c["properties"]["ADMIN"] != "Malta")]
    country_json["features"] = [malta..., europe...]

    country_geo = GeoJSON.read(JSON.json(country_json))
    return country_geo
end

begin
    source = "+proj=longlat +datum=WGS84"
    dest = "+proj=natearth2"
    # {type = :azimuthalEqualArea, scale = 525, center = [15, 53]},

    fig = Figure(resolution = (1000,500))
    ga = GeoAxis(
        fig[1, 1];
        source = source,
        dest = dest
    )

    ga.xticklabelsvisible[] = false
    ga.yticklabelsvisible[] = false

    eu_geojson = generate_esef_basemap(country_rollup)

    report_count_vect = map(eu_geojson) do geo
        report_count = (@chain country_rollup @subset(:country == geo.ADMIN) @select(:report_count))
        nrow(report_count) > 0 ? report_count[1, 1] : missing
    end

    max_reports = maximum(country_rollup[!, :report_count])

    for (c, report_count) in zip(eu_geojson, report_count_vect)
        poly!(ga, GeoMakie.geo2basic(c);
            strokecolor = :white,
            strokewidth = 1,
            color= colormap("Blues", max_reports)[report_count],
        )
    end
    fig
end


GeoMakie.geo2basic(eu_geojson[1])
