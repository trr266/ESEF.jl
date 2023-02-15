FROM julia:1.9-rc

RUN apt-get update && \
    apt-get install -y git

WORKDIR /app

COPY . /app/

RUN julia --project=. src/setup_julia.jl

ENV PATH=/root/.julia/conda/3/bin:$PATH
