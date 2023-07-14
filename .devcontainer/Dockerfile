FROM julia:1.10-rc

ENV JULIA_DEPOT_PATH=/opt/julia
ENV JULIA_PKGDIR=/opt/julia

WORKDIR /app

COPY . /app/

RUN julia --project=. src/setup_julia.jl
