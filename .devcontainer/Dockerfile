FROM julia:1.9-rc

ENV JULIA_DEPOT_PATH=/opt/julia
ENV JULIA_PKGDIR=/opt/julia
ENV CONDA_JL_HOME=/opt/julia/conda
ENV PATH=/opt/julia/conda/bin:$PATH

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -qq install -y git

WORKDIR /app

COPY . /app/

RUN julia --project=test src/setup_julia.jl
