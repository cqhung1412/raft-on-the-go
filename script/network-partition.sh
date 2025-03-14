#!/bin/bash

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root"
    exit 1
fi

# Cleans up existing iptables rules by flushing all rules, deleting user-defined chains,
# and resetting the default policies for the INPUT, FORWARD, and OUTPUT chains to ACCEPT.
#
# This function restores iptables to a neutral state, ensuring that no residual rules interfere
# with subsequent network partition operations.
#
# Globals:
#   None
#
# Arguments:
#   None
#
# Outputs:
#   Writes a message to STDOUT indicating that the cleanup process has started.
#
# Example:
#   cleanup_rules
cleanup_rules() {
    echo "Cleaning up existing iptables rules..."
    iptables -F
    iptables -X
    iptables -P INPUT ACCEPT
    iptables -P FORWARD ACCEPT
    iptables -P OUTPUT ACCEPT
}

# Creates iptables rules to partition network traffic between two sets of ports.
#
# This function sets up rules to block TCP traffic between two defined port groups:
#   - Partition 1: Ports 5001, 5002, and 5003
#   - Partition 2: Ports 5004 and 5005
#
# It appends iptables DROP rules for both incoming and outgoing packets, effectively isolating
# the two partitions by preventing bidirectional communication.
#
# Globals:
#   None
#
# Arguments:
#   None
#
# Outputs:
#   Updates the iptables configuration by appending new DROP rules.
#
# Example:
#   # Partition network traffic between the specified port ranges
#   create_partition_rules
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

# Displays the current iptables rules in verbose format.
#
# Globals:
#   None
#
# Arguments:
#   None
#
# Outputs:
#   Prints a header message followed by the detailed iptables rules to STDOUT.
#
# Example:
#   verify_rules
verify_rules() {
    echo "Current iptables rules:"
    iptables -L -n -v
}

# Removes network partitioning rules by resetting iptables.
#
# This function calls cleanup_rules to flush the existing iptables rules and reset the
# default policies for the INPUT, OUTPUT, and FORWARD chains. It then prints a message
# indicating that network partitioning has been removed.
#
# Globals:
#   None.
#
# Arguments:
#   None.
#
# Outputs:
#   Prints a confirmation message to STDOUT.
#
# Returns:
#   The exit status of cleanup_rules.
#
# Example:
#   $ remove_partition
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