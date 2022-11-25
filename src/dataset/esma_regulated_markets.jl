using Chain
using DataFrames
using JSON
using DataFrameMacros
using HTTP
using YAML
using Memoization

@memoize function get_regulated_markets_esma()
    reg_market_api_query = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"

    @chain reg_market_api_query begin
        HTTP.get(;
            query=Dict(
                "q" => "{!join from=id to=_root_}ae_entityTypeCode:MIR",
                "indent" => "true",
                "fq" => "(type_s:parent)",
                "rows" => "1000",
                "wt" => "json",
            ),
        )

        # Check 200 HTTP status code
        @aside @assert(_.status == 200)
        _.body
        String()
        JSON.parse()
        # Check that everything fit in one page
        @aside @assert(_["response"]["numFound"] < 1000)

        # Extract the data
        [DataFrame(d) for d in _["response"]["docs"]]
        reduce(vcat, _; cols=:union)

        # Data munging
        @sort(:ae_entityName)
    end
end

function get_esma_regulated_countries()
    @chain get_regulated_markets_esma() begin
        @combine(:esma_countries = @bycol titlecase.(unique(:ae_homeMemberState)))
        @sort(:esma_countries)
    end
end
