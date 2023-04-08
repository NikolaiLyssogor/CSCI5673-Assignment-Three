sudo apt-get update -y && sudo apt-get upgrade -y && \
sudo apt install git-all -y && \
sudo apt-get install python3-pip -y && \
pip install Flask requests grpcio zeep spyne protobuf==3.20.3 && \
git clone https://github.com/NikolaiLyssogor/CSCI5673-Assignment-Two.git && \
cd CSCI5673-Assignment-Two