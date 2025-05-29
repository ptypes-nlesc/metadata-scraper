#!/bin/bash

# Define a list of actual supported country names
COUNTRIES=(
    "United_States"
    "United_Kingdom"
    "Germany"
    "Netherlands"
    "Sweden"
    "Switzerland"
    "Canada"
    "France"
    "Australia"
    "Singapore"
)

# Disconnect current VPN session
echo "Disconnecting VPN..."
nordvpn disconnect
sleep 3

# Pick a random country
RANDOM_COUNTRY=${COUNTRIES[$RANDOM % ${#COUNTRIES[@]}]}
echo "Connecting to $RANDOM_COUNTRY..."

# Connect
nordvpn connect "$RANDOM_COUNTRY"
sleep 5
