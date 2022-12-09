FROM julia:1.8

RUN apt-get update && apt-get install -y git build-essential & \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate()'
