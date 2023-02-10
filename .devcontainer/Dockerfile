FROM julia:1.9-rc

RUN apt-get update && \
    apt-get install -y git

COPY src/ src/
COPY Project.toml .

RUN julia --project=. setup_julia.jl

ENV PATH=/root/.julia/conda/3/bin:$PATH
