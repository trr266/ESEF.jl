module ESEF

import Base: @invokelatest

include("helpers/flatten_dict.jl")
include("helpers/truncate_text.jl")

include("dataset/esma_regulated_markets.jl")
include("dataset/iso_country_codes.jl")

include("wikidata/quick_statements.jl")
include("wikidata/sparql_api.jl")
include("wikidata/object_lookup.jl")
include("wikidata/export_company_facts.jl")
include("wikidata/iso_country_table.jl")

include("lei/gleif_api.jl")
include("lei/gleif_to_wikidata_quickstatements.jl")

include("xbrl/esef_filings_api.jl")

include("local_sparql_db/sparql.jl")
include("local_sparql_db/oxigraph_server.jl")
include("local_sparql_db/load_esef_db.jl")

include("exploration/analysis.jl")
include("exploration/visualizations.jl")

end
