#!/bin/bash

# need to run this script as sudo
if [ "$(whoami)" != "root" ]; then
    echo "this script must be used with sudo"
    exit 1
fi

set -e
num_insts=$1
if [ -z $num_insts ]; then
    num_insts=4
fi

suffix=$2
addname=$3 #just pass any value

apt-get update && apt-get upgrade -y
apt-get install qemu-kvm libvirt-bin virtinst cloud-utils -y

image=bionic-server-cloudimg-amd64.img 
if [ ! -f $image ]; then
    wget https://cloud-images.ubuntu.com/bionic/current/${image}
fi

image_dir=/var/lib/libvirt/images
if [ ! -d $image_dir ]; then
    mkdir -p $image_dir 
fi

config_dir=configs
if [ ! -d $config_dir ]; then
    mkdir $config_dir
fi
cloud_config=$(cat <<-END
	#cloud-config
	password: cloudpass
	chpasswd: { expire: False }
	ssh_pwauth: True
	hostname: 
END
)

# function must have 
#   defined vals: image, image_dir, cloud_config, config_dir
#   params: name of node, cpu size, ram cores
function create_node() {
    node=$1
    cpu_amt=$2
    ram_size=$3

    qemu-img convert -f qcow2 $image ${image_dir}/${node}.img
    echo "${cloud_config}${node}" > ${config_dir}/${node}.txt  #add hostname

    cloud-localds ${image_dir}/${node}.iso ${config_dir}/${node}.txt

    virt-install --name $node --ram $ram_size --vcpus $cpu_amt \
        --disk ${image_dir}/${node}.img,device=disk,bus=virtio \
        --disk ${image_dir}/${node}.iso,device=cdrom --os-type linux \
        --os-variant ubuntu16.04 --virt-type kvm --graphics none \
        --network network=default,model=virtio --import --noreboot
}

#create name node
if [ ! -z $addname ]; then
    disk=15
    ram=16384
    cpu=8
    qemu-img resize $image ${disk}G
    echo "making name node with ${cpu} cpu cores, ${ram}MB of ram, ${disk}GB disk"
    create_node "namenode" $cpu $ram
fi

#create data nodes
disk=$((180/num_insts))
ram=$((32768/num_insts))
cpu=$((16/num_insts))
qemu-img resize $image ${disk}G
echo "using instance size ${disk}GB, ${ram}MB of ram, ${cpu} cpu cores"

for (( i=1; i<=$num_insts; ++i )); do
    new_node="datanode-${suffix}${i}"
    create_node $new_node $cpu $ram
done

echo "finished creating ${num_insts} vms"

