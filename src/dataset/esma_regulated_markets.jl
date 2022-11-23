using Chain
using DataFrames
using JSON
using DataFrameMacros
using HTTP
using YAML

function get_regulated_markets_esma()
    reg_market_query = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select?q=%7B!join+from%3Did+to%3D_root_%7Dae_entityTypeCode%3AMIR&fq=(type_s%3Aparent)&rows=1000&wt=json&indent=true"

    @chain reg_market_query begin
        HTTP.get
        # Check 200 HTTP status code
        @aside @assert(_.status == 200)
        _.body
        String()
        JSON.parse()
        # Check that everything fit in one page
        @aside @assert(_["response"]["numFound"] < 1000)
    end

    df = DataFrame()
    for d in raw_data["response"]["docs"]
        append!(df, DataFrame(d))
    end

    # Strip out excess columns
    df = @chain df begin
        @select(:id, :ae_entityName, :ae_competentAuthority)
        @transform(
            :url = "TODO",
            :time_estimate = "TODO",
            :scraping_type = "TODO",
            :secondary = false
        )
        @sort(:ae_entityName)
    end

    return df
end
