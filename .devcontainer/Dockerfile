FROM julia:1.11

ENV JULIA_DEPOT_PATH=/opt/julia
ENV JULIA_PKGDIR=/opt/julia

RUN apt-get update && \
    apt-get install -y git

WORKDIR /app

COPY . /app/

RUN julia --project=. src/setup_julia.jl
