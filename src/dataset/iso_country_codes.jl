using Chain
using HTTP
using DataFrames
using DataFrameMacros
using CSV

function get_country_codes()
    country_lookup_url = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"

    country_lookup = @chain country_lookup_url begin
        HTTP.get()
        _.body
        CSV.read(DataFrame; normalizenames=true)
        @select(:country = :name, :country_alpha_2 = :alpha_2, :region)
        # Rename "United Kingdom of Great Britain and Northern Ireland" to "United Kingdom" for visualization legibility
        @transform!(@subset(:country_alpha_2 == "GB"), :country = "United Kingdom")
        # Country missing...
        push!(["Czechoslovakia", "CS", "Europe"])
    end

    return country_lookup
end
