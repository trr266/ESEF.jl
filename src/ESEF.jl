module ESEF

import Base: @invokelatest

include("sparql.jl")
include("query_wikidata.jl")
include("query_lei.jl")
include("iso_country_codes.jl")
include("wikidata_public_companies.jl")
include("esef_index_viz.jl")
include("esef_wikidata_exploration.jl")
include("esef_xbrl_filings.jl")
include("esma_regulated_markets.jl")
include("oxigraph_server.jl")
include("query_wikidata_facts.jl")
include("esef_xml_parse.jl")

end
