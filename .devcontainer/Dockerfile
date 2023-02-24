FROM julia:1.9-rc

ENV JULIA_DEPOT_PATH=/opt/julia
ENV JULIA_PKGDIR=/opt/julia
ENV CONDA_JL_HOME=/opt/julia/conda
ENV PATH=/opt/julia/conda/bin:/root/.cargo/bin:$PATH

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -qq install -y git build-essential clang
WORKDIR /app

COPY . /app/

RUN julia --project=test src/setup_julia.jl
RUN julia --project=. src/setup_julia.jl
