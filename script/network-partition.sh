#!/bin/bash

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root"
    exit 1
fi

# Function to cleanup existing rules
cleanup_rules() {
    echo "Cleaning up existing iptables rules..."
    iptables -F
    iptables -X
    iptables -P INPUT ACCEPT
    iptables -P FORWARD ACCEPT
    iptables -P OUTPUT ACCEPT
}

# Function to create partition rules
create_partition_rules() {
    # Block traffic between partitions
    # Block Partition 1 (5001-5003) from reaching Partition 2 (5004-5005)
    for source_port in 5001 5002 5003; do
        for dest_port in 5004 5005; do
            iptables -A INPUT -p tcp --sport $source_port --dport $dest_port -j DROP
            iptables -A OUTPUT -p tcp --sport $source_port --dport $dest_port -j DROP
        done
    done

    # Block Partition 2 (5004-5005) from reaching Partition 1 (5001-5003)
    for source_port in 5004 5005; do
        for dest_port in 5001 5002 5003; do
            iptables -A INPUT -p tcp --sport $source_port --dport $dest_port -j DROP
            iptables -A OUTPUT -p tcp --sport $source_port --dport $dest_port -j DROP
        done
    done
}

# Function to verify rules
verify_rules() {
    echo "Current iptables rules:"
    iptables -L -n -v
}

# Function to remove partitioning
remove_partition() {
    cleanup_rules
    echo "Network partitioning removed"
}

# Main script
case "$1" in
    "create")
        echo "Creating network partition..."
        cleanup_rules
        create_partition_rules
        verify_rules
        echo "Network partition created successfully"
        ;;
    "remove")
        echo "Removing network partition..."
        remove_partition
        verify_rules
        ;;
    *)
        echo "Usage: $0 {create|remove}"
        exit 1
        ;;
esac