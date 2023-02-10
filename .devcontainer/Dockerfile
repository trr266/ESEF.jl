FROM julia:1.8

RUN apt-get update && \
    apt-get install -y git

COPY src/setup_julia.jl .

RUN julia --project=. setup_julia.jl

ENV PATH=/root/.julia/conda/3/bin:$PATH
