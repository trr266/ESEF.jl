FROM julia:1.9-rc

RUN apt-get update && \
    apt-get install -y build-essential git clang && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate()'
