sudo apt update

sudo apt install build-essential
sudo apt install libopenmpi-dev openmpi-bin # libopenmpi-dev is needed for mpi4py 

sudo apt-get install nodejs npm
curl -fsSL https://deno.land/install.sh | sh

echo "Setting up the mamba environment via miniforge"

wget "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
bash Miniforge3-Linux-x86_64.sh

mamba env create -f environment.yml # Includes everything for webilastik env now
