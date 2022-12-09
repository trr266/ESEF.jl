FROM julia:1.8

RUN apt-get update && apt-get install -y git cargo

RUN julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate()'
