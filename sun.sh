#!/bin/bash

# sun - Run Rust projects across Sunlab cluster nodes

set -e

# Configuration
USERNAME="jod323"
DOMAIN="cse.lehigh.edu"
LOG_DIR="logs"
PORT="1895"
# scratch is a larger partition on sunlab that resides per-machine
TARGET_DIR="/scratch/.cargo/${USERNAME}/target"

# All known Sunlab nodes
ALL_NODES=(
    ariel caliban callisto ceres
    chiron cupid eris europa hydra
    iapetus io ixion mars mercury
    neptune nereid nix orcus phobos puck
    saturn triton varda vesta xena
)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
    cat <<'EOF'
    Usage: sun.sh <command> [options]

    Commands:
    list                              List all nodes with their current CPU load
    run -n <num> [-- args...]         Run 'target/release/dht' on N least-loaded nodes
    exec -n <num> -- <command>        Run arbitrary command on N least-loaded nodes
    kill [name]                       Kill processes on all nodes (default: 'dht')

    Options:
    -n <num>      Number of nodes to use (required for 'run' and 'exec')
    -k <num>      Number of keys for benchmark (passed to dht)
    -r <num>      Key range for benchmark (passed to dht)
    -d <dir>      Project directory (default: current directory)
    -v            Enable debug logging (RUST_LOG=debug instead of info)
    -h, --help    Show this help

    Examples:
    sun.sh list
    sun.sh run -n 3
    sun.sh run -n 3 -v                   Run with debug logging
    sun.sh run -n 3 -k 100000
    sun.sh run -n 3 -k 100000 -r 1000
    sun.sh run -n 3 -- --keys 1000 --ops 50000
    sun.sh run -n 5 -d ~/dev/cse476/project -- --config config.toml
    sun.sh exec -n 3 -- hostname
    sun.sh exec -n 5 -- "cd ~/dev/project && ./my_script.sh"
    sun.sh kill                          Kill 'dht' on all nodes
    sun.sh kill myapp                    Kill 'myapp' on all nodes

    Logs are saved to: logs/<node>.log

    Ctrl+C stops all nodes and cleans up.
EOF
    exit 0
}

# Get load and port status for a single node
# Output format: "load node port_status" where port_status is "free" or "busy"
probe_node() {
    local node=$1
    local host="${node}.${DOMAIN}"

    local result=$(ssh -o StrictHostKeyChecking=no \
        -o ConnectTimeout=3 \
        -o BatchMode=yes \
        "${USERNAME}@${host}" \
        "load=\$(cat /proc/loadavg | cut -d' ' -f1); \
         if ss -tlnp 2>/dev/null | grep -q ':${PORT} ' || netstat -tlnp 2>/dev/null | grep -q ':${PORT} '; then \
           port_status=busy; \
         else \
           port_status=free; \
         fi; \
         echo \"\$load \$port_status\"" 2>/dev/null)

    if [[ -n "$result" ]]; then
        local load=$(echo "$result" | cut -d' ' -f1)
        local port_status=$(echo "$result" | cut -d' ' -f2)
        echo "$load $node $port_status"
    fi
}

# Probe all nodes in parallel and return sorted by load
# Output format: "load node port_status" per line
get_sorted_nodes() {
    echo -e "${CYAN}Probing ${#ALL_NODES[@]} nodes for CPU load and port ${PORT}...${NC}" >&2

    local tmp_file=$(mktemp)

    for node in "${ALL_NODES[@]}"; do
        probe_node "$node" >>"$tmp_file" &
    done
    wait

    sort -n "$tmp_file"
    rm -f "$tmp_file"
}

# Kill my dht binary running on all machines
cmd_kill() {
    local binary_name="${1:-dht}"

    echo -e "${GREEN}=== Killing '$binary_name' on all nodes ===${NC}"
    echo ""

    for node in "${ALL_NODES[@]}"; do
        local host="${node}.${DOMAIN}"
        result=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o BatchMode=yes \
            "${USERNAME}@${host}" "pkill -u $USERNAME $binary_name && echo 'killed' || echo 'none'" 2>/dev/null)
        if [[ "$result" == "killed" ]]; then
            echo -e "${YELLOW}[$node]${NC} killed $binary_name"
        fi
    done &
    wait

    echo ""
    echo -e "${GREEN}Done${NC}"
}

# List command
cmd_list() {
    echo -e "${GREEN}=== Sunlab Node Status ===${NC}"
    echo ""
    printf "%-12s %-12s %s\n" "NODE" "LOAD (1min)" "PORT ${PORT}"
    printf "%-12s %-12s %s\n" "----" "----------" "--------"

    get_sorted_nodes | while read load node port_status; do
        if [[ "$port_status" == "busy" ]]; then
            color=$RED
            port_display="BUSY"
        elif (($(echo "$load < 1.0" | bc -l))); then
            color=$GREEN
            port_display="free"
        elif (($(echo "$load < 3.0" | bc -l))); then
            color=$YELLOW
            port_display="free"
        else
            color=$RED
            port_display="free"
        fi
        printf "${color}%-12s %-12s %s${NC}\n" "$node" "$load" "$port_display"
    done
    echo ""
}

# Select N least-loaded nodes with free ports
select_nodes() {
    local num_nodes=$1

    # Filter to only nodes with free ports, then take by load
    mapfile -t SORTED < <(get_sorted_nodes | grep ' free$')

    if [[ ${#SORTED[@]} -lt $num_nodes ]]; then
        echo -e "${RED}Error: Only ${#SORTED[@]} available nodes (with free port), but $num_nodes requested${NC}" >&2
        exit 1
    fi

    echo -e "${GREEN}Selected nodes (by lowest load, port ${PORT} free):${NC}" >&2
    printf "  %-12s %s\n" "NODE" "LOAD" >&2

    for i in $(seq 0 $((num_nodes - 1))); do
        load=$(echo "${SORTED[$i]}" | cut -d' ' -f1)
        node=$(echo "${SORTED[$i]}" | cut -d' ' -f2)
        printf "  ${CYAN}%-12s %s${NC}\n" "$node" "$load" >&2
        echo "$node"
    done
}

# Build --connections arg for a node (all other nodes, comma-separated)
get_connections() {
    local current_node=$1
    shift
    local all_nodes=("$@")

    local connections=""
    for node in "${all_nodes[@]}"; do
        if [[ "$node" != "$current_node" ]]; then
            if [[ -n "$connections" ]]; then
                connections="${connections},${node}"
            else
                connections="$node"
            fi
        fi
    done
    echo "$connections"
}

# Run commands on selected nodes
run_on_nodes() {
    local command="$1"
    shift
    local nodes=("$@")

    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    # Save run metadata
    cat >"$LOG_DIR/run_info.txt" <<EOF
Run started: $(date)
Command: $command
Nodes: ${nodes[*]}
EOF

    echo ""
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
    echo ""

    # Store PIDs for cleanup
    declare -A PIDS

    cleanup() {
        echo ""
        echo -e "${RED}Caught interrupt, stopping all nodes...${NC}"

        # Kill local SSH processes
        for node in "${!PIDS[@]}"; do
            kill "${PIDS[$node]}" 2>/dev/null && echo -e "${YELLOW}[$node]${NC} stopped"
        done

        # Kill remote processes
        for node in "${nodes[@]}"; do
            local host="${node}.${DOMAIN}"
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes \
                "${USERNAME}@${host}" "pkill -u $USERNAME dht" 2>/dev/null
        done

        echo ""
        echo -e "${GREEN}=== Cleanup Complete ===${NC}"
        echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    # Start all nodes in background
    for node in "${nodes[@]}"; do
        local host="${node}.${DOMAIN}"
        local log_file="$LOG_DIR/${node}.log"

        echo -e "${YELLOW}[$node]${NC} starting..."

        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "${USERNAME}@${host}" "$command" \
            >"$log_file" 2>&1 &

        PIDS[$node]=$!
    done

    echo ""
    echo -e "${GREEN}All nodes started. Waiting for completion...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all nodes${NC}"
    echo ""

    # Wait for all processes
    failed=0
    for node in "${!PIDS[@]}"; do
        if wait "${PIDS[$node]}"; then
            echo -e "${GREEN}[$node]${NC} completed"
        else
            echo -e "${RED}[$node]${NC} failed"
            ((failed++))
        fi
    done

    echo ""
    echo -e "${GREEN}=== Run Complete ===${NC}"
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"

    if [[ $failed -gt 0 ]]; then
        echo -e "${RED}$failed node(s) failed${NC}"
    fi
}

# Run command
cmd_run() {
    local num_nodes="$1"
    local project_dir="$2"
    local num_keys="$3"
    local key_range="$4"
    shift 4
    local program_args="$*"

    echo -e "${GREEN}=== Running on Cluster ===${NC}"
    echo ""

    echo -e "${CYAN}Building project to populate cargo cache...${NC}"
    (cd "$project_dir" && cargo build --release --quiet)
    echo -e "${GREEN}Build complete${NC}"
    echo ""

    mapfile -t nodes < <(select_nodes "$num_nodes")

    echo ""
    echo -e "Directory: ${BLUE}$project_dir${NC}"
    if [[ -n "$num_keys" ]]; then
        echo -e "Num keys: ${BLUE}$num_keys${NC}"
    fi
    if [[ -n "$key_range" ]]; then
        echo -e "Key range: ${BLUE}$key_range${NC}"
    fi
    if [[ -n "$program_args" ]]; then
        echo -e "Extra args: ${BLUE}$program_args${NC}"
    fi

    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    echo ""
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
    echo ""

    declare -A PIDS

    cleanup() {
        echo ""
        echo -e "${RED}Caught interrupt, stopping all nodes...${NC}"

        for node in "${!PIDS[@]}"; do
            kill "${PIDS[$node]}" 2>/dev/null && echo -e "${YELLOW}[$node]${NC} stopped"
        done

        for node in "${nodes[@]}"; do
            local host="${node}.${DOMAIN}"
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes \
                "${USERNAME}@${host}" "pkill -u $USERNAME dht" 2>/dev/null
        done

        echo ""
        echo -e "${GREEN}=== Cleanup Complete ===${NC}"
        echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    for node in "${nodes[@]}"; do
        local host="${node}.${DOMAIN}"
        local log_file="$LOG_DIR/${node}.log"
        local connections
        connections=$(get_connections "$node" "${nodes[@]}")

        local cmd="cd $project_dir && cargo build --release --quiet --target-dir $TARGET_DIR && RUST_LOG=info $TARGET_DIR/release/dht --name $node --connections $connections"
        if [[ -n "$num_keys" ]]; then
            cmd="$cmd --num-keys $num_keys"
        fi
        if [[ -n "$key_range" ]]; then
            cmd="$cmd --key-range $key_range"
        fi
        if [[ -n "$program_args" ]]; then
            cmd="$cmd $program_args"
        fi

        echo -e "${YELLOW}[$node]${NC} starting (connects to: $connections)"

        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "${USERNAME}@${host}" "$cmd" \
            >"$log_file" 2>&1 &

        PIDS[$node]=$!
    done

    echo ""
    echo -e "${GREEN}All nodes started. Waiting for completion...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all nodes${NC}"
    echo ""

    failed=0
    for node in "${!PIDS[@]}"; do
        if wait "${PIDS[$node]}"; then
            echo -e "${GREEN}[$node]${NC} completed"
        else
            echo -e "${RED}[$node]${NC} failed"
            ((failed++))
        fi
    done

    echo ""
    echo -e "${GREEN}=== Run Complete ===${NC}"
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"

    if [[ $failed -gt 0 ]]; then
        echo -e "${RED}$failed node(s) failed${NC}"
    fi
}

# Exec command
cmd_exec() {
    local num_nodes="$1"
    shift
    local command="$*"

    if [[ -z "$command" ]]; then
        echo -e "${RED}Error: No command specified after --${NC}"
        exit 1
    fi

    echo -e "${GREEN}=== Executing on Cluster ===${NC}"
    echo ""

    mapfile -t nodes < <(select_nodes "$num_nodes")

    echo ""
    echo -e "Command: ${BLUE}$command${NC}"

    run_on_nodes "$command" "${nodes[@]}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if [[ $# -eq 0 ]]; then
    usage
fi

COMMAND="$1"
shift

NUM_NODES=""
PROJECT_DIR="$(pwd)"
NUM_KEYS=""
KEY_RANGE=""
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
    -n)
        NUM_NODES="$2"
        shift 2
        ;;
    -k)
        NUM_KEYS="$2"
        shift 2
        ;;
    -r)
        KEY_RANGE="$2"
        shift 2
        ;;
    -d)
        PROJECT_DIR="$2"
        shift 2
        ;;
    -v)
        RUST_LOG="debug"
        shift
        ;;
    -h | --help)
        usage
        ;;
    --)
        shift
        EXTRA_ARGS="$*"
        break
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        usage
        ;;
    esac
done

case "$COMMAND" in
list)
    cmd_list
    ;;
run)
    if [[ -z "$NUM_NODES" ]]; then
        echo -e "${RED}Error: -n <num_nodes> is required for 'run'${NC}"
        echo ""
        usage
    fi
    if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [[ "$NUM_NODES" -lt 1 ]]; then
        echo -e "${RED}Error: Number of nodes must be a positive integer${NC}"
        exit 1
    fi
    cmd_run "$NUM_NODES" "$PROJECT_DIR" "$NUM_KEYS" "$KEY_RANGE" $EXTRA_ARGS
    ;;
exec)
    if [[ -z "$NUM_NODES" ]]; then
        echo -e "${RED}Error: -n <num_nodes> is required for 'exec'${NC}"
        echo ""
        usage
    fi
    if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [[ "$NUM_NODES" -lt 1 ]]; then
        echo -e "${RED}Error: Number of nodes must be a positive integer${NC}"
        exit 1
    fi
    if [[ -z "$EXTRA_ARGS" ]]; then
        echo -e "${RED}Error: 'exec' requires a command after --${NC}"
        echo ""
        usage
    fi
    cmd_exec "$NUM_NODES" $EXTRA_ARGS
    ;;
kill)
    cmd_kill "${EXTRA_ARGS:-dht}"
    ;;
-h | --help)
    usage
    ;;
*)
    echo -e "${RED}Unknown command: $COMMAND${NC}"
    echo ""
    usage
    ;;
esac
