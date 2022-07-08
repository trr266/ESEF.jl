# ESEF.jl

`ESEF.jl` is a package which collects functions for manipulating ESEF format data.

The ESEF data pipeline workflow is as follows:

1. Collect all available ESEF files from the filings.xbrl.org repository
2. Extract 'facts' from these files and generate .nt RDF items from the facts
3. Collect adjacent facts about corporate entities from Wikidata (using query API)
4. Load oxigraph RDF / SPARQL database with the collected data
5. Run queries against the dataset, returning standardized data, ready to go for research
