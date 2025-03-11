git checkout https://github.com/robtandy/datafusion-ray
cd datafusion-ray
git checkout k8s_benchmarking

sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker   


sudo apt install -y python3-virtualenv

virtualenv -p $(which python3) my_venv
. ./my_venv/bin/activate    
pip install click jinja2 pandas datafusion 'ray[default]'

mkdir /data
sudo chmod -R 777 /data
sudo chown -R ubuntu:ubuntu

