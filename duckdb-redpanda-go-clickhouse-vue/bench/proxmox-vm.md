# One benchmark VM on Proxmox: setup and reading the results

This guide sets up a single Proxmox VM that gets (nearly) all of the machine's CPU cores. The
benchmark then varies the cores inside it: `bench/matrix.sh` pins each service to its own cores
with Docker's `cpuset` (`compose.perf.yaml`), for a matrix of pipeline cores × `REPLAY_COPIES`.

The weak spot of this design is the top of the curve: runs where the pipeline gets almost all of
the VM's cores. There, the pipeline starts to share cores with things the benchmark doesn't
count, inside the VM and on the host. Part 7 lists what to check before trusting those points.

Labels are from Proxmox VE 8 and 9; other versions may differ slightly. Steps marked **(shell)**
need the host's shell: in the UI, select the node, then **Shell**.

## 1. Before you start

Write these down; later steps use them.

- **Host CPU.** Select the node, then **Summary**: the CPU model, sockets, cores and threads. In
  **(shell)**, `lscpu` gives the same, and `lscpu -e` shows which CPU numbers are hyperthreads of
  the same physical core.
- **Storage.** Use local NVMe if there is one. In **Datacenter → Storage**, note the storage
  that sits on it (typically `local-lvm`, which is LVM-thin; or `local-zfs`).
- **Memory.** At least 32 GB for the VM, better 64 GB. At the largest core counts, Redpanda gets
  1 GiB per core (`bench/matrix.sh` sets this), and ClickHouse, the signal service and the
  replayer each take several GB at `REPLAY_COPIES=20`.
- **Disk.** About 200 GB: the OS, the images, the data, and the largest run (about 0.8 GB per
  copy for a full Monday, twice that while ClickHouse merges).
- **Nothing else on the host** while benchmarks run: stop other VMs and containers, and check
  **Datacenter → Backup** and the task log for scheduled jobs.

## 2. Host settings for benchmark sessions (shell)

None of these survive a reboot, which is what you want: reboot to return to normal.

**Hyperthreading off.** Recommended for scaling runs. With it on, two "cores" in the VM can
be the two threads of one physical core, worth about 1.2–1.3 cores rather than 2, and the curve
mixes the two. Turning it off makes every vCPU a whole core:

```sh
echo off > /sys/devices/system/cpu/smt/control
nproc            # now the number of physical cores
```

If you keep it on, say so in the report and read every count as hardware threads.

**Fixed CPU speed.** Turbo clocks a few busy cores higher than many, which flattens the scaling
curve for reasons outside the software:

```sh
apt install -y linux-cpupower
cpupower frequency-set -g performance
echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo      # Intel
echo 0 > /sys/devices/system/cpu/cpufreq/boost              # AMD (and acpi-cpufreq)
```

Only one of the last two lines exists on a given machine. If you can't turn turbo off, record it.

**Measuring tools** for parts 6 and 7: `apt install -y sysstat linux-perf`. That gives `pidstat` and `perf`; `turbostat` comes with `linux-cpupower` above.

## 3. Create the VM

### 3.1 Upload the installer

Select the node, then storage **local**, then **ISO Images**, then **Download from URL** (or
**Upload**). Use Debian 13 (netinst) or Ubuntu Server 24.04/26.04 LTS. Clone every future
benchmark VM from this one, so the kernel and Docker versions stay the same.

### 3.2 The Create VM wizard

Click **Create VM** (top right) and tick **Advanced** at the bottom of the dialog, so every
field below is shown.

| Tab | Field | Value | Why |
| --- | --- | --- | --- |
| General | Name | `bench` | |
| | Start at boot | off | The VM runs only during benchmark sessions |
| OS | ISO image | the uploaded installer | |
| | Type / Version | Linux, 6.x – 2.6 Kernel | |
| System | Machine | q35 | |
| | BIOS | SeaBIOS (simplest) or OVMF (UEFI, then add the EFI disk on the NVMe storage) | |
| | SCSI Controller | **VirtIO SCSI single** | Gives the disk its own I/O thread |
| | Qemu Agent | **on** | Clean shutdowns, and the IP address shows in the UI |
| | Add TPM | off | |
| Disks | Bus/Device | SCSI 0 | |
| | Storage | the NVMe-backed storage | Same storage for every run you compare |
| | Disk size | 200 GiB | Part 1 |
| | Cache | **Default (No cache)** | Flushes reach the disk. "Write back" or "unsafe" make Redpanda look faster than it is |
| | Discard | on | |
| | IO thread | **on** | |
| | SSD emulation | on | |
| | Async IO | io_uring (default) | |
| | Backup | off | It's a throwaway VM, and its disk gets large |
| CPU | Sockets | 1 | |
| | Cores | the host's cores, minus one (see below) | |
| | Type | **host** | The default types hide instructions such as AVX2, which ClickHouse uses heavily |
| | CPU limit | 0 (unlimited) | A limit throttles bursts, like Docker's `--cpus` |
| | CPU units | default | |
| | CPU Affinity | empty (part 4 pins instead) | |
| | Enable NUMA | on only with two or more CPU sockets | |
| Memory | Memory (MiB) | 65536 (or at least 32768) | Part 1 |
| | Minimum memory | the same value | |
| | Ballooning Device | **off** | The host must not take memory back during a run |
| Network | Bridge | vmbr0 | |
| | Model | VirtIO (paravirtualized) | |
| | Firewall | off | |

Don't tick **Start after created**.

**Cores: all, or all but one?** The idea is to give the VM the whole machine, but the host
still has work to do while it runs:
- the Proxmox services
- QEMU's own threads, including the disk's I/O thread
- interrupts

With every core given to the VM, that work takes time from some vCPU, and Linux in the VM sees
it as stolen time. Keeping one core back for the host avoids most of this, so that's the
default here: **Cores = physical cores − 1** (with hyperthreading off). If you give it all of
them, part 7's steal-time check becomes essential.

### 3.3 After the wizard

- **Hardware → Processors:** the row should show the core count and `[host]`.
- **Hardware → Memory:** check that ballooning is off.
- **Options → QEMU Guest Agent:** Enabled.

## 4. Pin each vCPU to one host core (recommended)

The **CPU Affinity** field in the UI restricts the VM as a whole to a set of host CPUs. Within
that set, the host still moves the vCPUs around, so a container pinned to "core 6" in the VM
isn't on a fixed physical core.

A Proxmox hookscript can pin every vCPU one-to-one when the VM starts, and put QEMU's other
threads (its main loop and the disk I/O thread) on the core kept for the host. Hookscripts can
only be attached in the shell.

1. **Allow snippets on `local`:** **Datacenter → Storage → local → Edit → Content**, and add
   **Snippets**.
2. **Write the script (shell)** to `/var/lib/vz/snippets/pin-vcpus.sh`. `HOST_CORE` is the core
   kept for the host, and `FIRST_PIN` is the host core that vCPU 0 goes to. With hyperthreading
   off and the VM given cores 1..N, the defaults map vCPU i to host core i+1:

   ```bash
   #!/bin/bash
   # Proxmox hookscript: pin vCPU i to host core FIRST_PIN+i, and QEMU's other
   # threads to HOST_CORE, after the VM has started
   vmid=$1 phase=$2
   [ "$phase" = post-start ] || exit 0
   HOST_CORE=0
   FIRST_PIN=1
   pid=$(cat "/var/run/qemu-server/$vmid.pid")
   for t in /proc/"$pid"/task/*; do
     tid=${t##*/}
     if [[ $(cat "$t/comm") =~ ^CPU\ ([0-9]+)/KVM$ ]]; then
       taskset -pc $(( FIRST_PIN + BASH_REMATCH[1] )) "$tid" >/dev/null
     else
       taskset -pc "$HOST_CORE" "$tid" >/dev/null
     fi
   done
   ```

3. **Attach it (shell):**

   ```sh
   chmod +x /var/lib/vz/snippets/pin-vcpus.sh
   qm set <vmid> --hookscript local:snippets/pin-vcpus.sh
   ```

4. **Check it after the VM starts (shell).** Each `CPU n/KVM` thread should show its own core:

   ```sh
   pid=$(cat /var/run/qemu-server/<vmid>.pid)
   for t in /proc/$pid/task/*; do printf '%-16s %s\n' "$(cat $t/comm)" "$(taskset -pc ${t##*/} | cut -d: -f2)"; done
   ```

QEMU threads that start later (for example worker threads for disk I/O) aren't covered by the
script. The `pidstat` log from part 6 shows whether any of them matter.

## 5. Inside the VM

Start the VM (**Start**, then **Console**) and install the OS with an SSH server and no desktop.
Then, as root:

```sh
apt install -y qemu-guest-agent tmux git sysstat procps fio
systemctl enable --now qemu-guest-agent

# No swap, and no background updates during runs
swapoff -a && sed -i '/\sswap\s/s/^/#/' /etc/fstab
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer unattended-upgrades 2>/dev/null

# Interrupts on the load generator's cores (0-3), off the pipeline's
systemctl disable --now irqbalance 2>/dev/null
echo f > /proc/irq/default_smp_affinity        # hex mask: cores 0-3
for f in /proc/irq/*/smp_affinity_list; do echo 0-3 > "$f" 2>/dev/null; done
```

The interrupt settings don't survive a reboot; run those three lines again after each boot.

**Docker Engine:** install it from Docker's own apt repository, following
https://docs.docker.com/engine/install/debian/ (or `/ubuntu/`). Then add your user to the
`docker` group and log in again.

**Check the VM** before the first run:

```sh
nproc                                   # the Cores you set
grep -o -m1 avx2 /proc/cpuinfo          # "avx2": CPU type host is in effect
docker info --format '{{.NCPU}}'        # the same as nproc
vmstat 5 3                              # "st" (stolen) column 0 while idle
fio --name=fsync --filename=/var/lib/docker/fio.test --rw=write --bs=4k --size=256M \
    --ioengine=sync --fdatasync=1 --runtime=30 --time_based && rm /var/lib/docker/fio.test
```

Write down fio's `sync` latency percentiles: they're the storage's flush latency, which every
result depends on.

**The project and data:**

```sh
git clone git@github.com:ohtopihlajamaki/trading-trends.git && cd trading-trends
mkdir -p data && rsync -a <laptop>:GitHub/trading-trends/data/ticks/ data/ticks/   # the prepped Parquet
cd duckdb-redpanda-go-clickhouse-vue
cp .env.example .env    # set CLICKHOUSE_PASSWORD; bench/matrix.sh sets the CPUS_* itself
```

To prep more days in the VM instead, copy `data/raw` (about 23 GB) and run
`docker compose --profile prep run --rm prep` with `DAYS` set.

## 6. Running the matrix

With V vCPUs, `bench/matrix.sh` keeps the load generator (the replayer and the probe) on cores
0–3, and gives the pipeline cores 4 to 4+N−1. So the largest possible N is V − 4.

**First, a dry run and a quick check:**

```sh
MATRIX_DRY_RUN=1 CORES="4 8 16 28" bench/matrix.sh
MATRIX_NO_TMUX=1 CORES=4 COPIES=1 REPEATS=1 REALTIME_POINTS=p50 REALTIME_SPEED=20 bench/matrix.sh
```

**Then the matrix.** For example, with V = 32:

```sh
CORES="4 8 12 16 20 24 26 28" COPIES="1 5 10 20" REPEATS=3 bench/matrix.sh
tmux attach -t bench-matrix
```

- Include a few points close to the top (here 24, 26 and 28), so you can see where the curve
  bends.
- Expect roughly 20 minutes per run at k = 1 and 45 at k = 20.
- Results, logs and per-run `mpstat`/`vmstat` files go to `bench/results/matrix-<host>-<time>/`.

**On the host, during the runs at the top of the curve (shell)**, record what QEMU's threads
are doing:

```sh
pidstat -t -p $(cat /var/run/qemu-server/<vmid>.pid) 5 > /root/qemu-threads.log
turbostat --quiet --interval 5 --show Busy%,Bzy_MHz > /root/turbostat.log   # core speed
```

## 7. Reading the top of the curve critically

As N approaches V − 4, the pipeline gets cores that, in smaller runs, were idle and soaked up
everything else. Each row below is a reason a high-N point can come out too low, or too noisy,
for reasons that aren't the pipeline's scaling.

| What happens | How to see it | What to do |
| --- | --- | --- |
| **The host takes time from vCPUs** (Proxmox services, QEMU's threads, interrupts) | `.vmstat`: the `st` column above ~1%. On the host: `pidstat` showing QEMU's non-vCPU threads busy | Keep a host core (part 3.2) and pin (part 4). Discard runs with steal |
| **The VM's own system work lands on pipeline cores** (the kernel, dockerd, containerd, Docker's network bridge, journald) | `.mpstat`: `%sys`, `%irq` or `%soft` on pipeline cores, or load on cores no service owns | Interrupts on 0–3 (part 5). Compare N = V−4 with N = V−6, which leaves two cores free |
| **The load generator is the limit**, not the pipeline | The log's `replay_max_lag_ms` well above 0, or cores 0–3 near 100% in `.mpstat` | Give it more cores (`LOAD_CPUS=0-5`, `FIRST_CORE=6`) and rerun those points |
| **Two vCPUs share a physical core** | Hyperthreading on, or no one-to-one pinning | Part 2 and part 4 |
| **Lower clock speed with more busy cores** | `turbostat`: `Bzy_MHz` falls as N grows | Turbo off (part 2), or report the speed next to each point |
| **The disk or its I/O thread is the limit**, from Redpanda's flushes and ClickHouse's merges on one virtual disk | `.vmstat`: high `wa`. In the VM: `iostat -x 5` with growing `w_await`. On the host: the QEMU I/O thread near 100% | A second virtual disk (with its own I/O thread) for `/var/lib/docker` |
| **Memory bandwidth and shared cache** run out | `perf stat -e cache-misses` on the host, or throughput per core falling while every check above is clean | Real hardware scaling: report it as such |
| **One service gets cores it can't use** (the split gives ClickHouse the remainder) | `.mpstat`: some services' cores idle while others are at 100% | Change `split()` in `bench/matrix.sh` and rerun |

**Rules for the report:**

1. **Plot throughput per core next to throughput.** A real scaling limit bends both; a
   benchmark artefact usually shows up as a sudden drop at the last one or two points.
2. **Only plot runs that pass:**
   - steal under 1%
   - `replay_max_lag_ms` near 0
   - `dropped` 0, and bars received equal to bars expected
3. **Repeats must agree.** If the repeats of one point spread by more than about 10%, that point
   is noise, whatever its median says.
4. **Say how the machine was set up:**
   - the host CPU model, hyperthreading on or off, turbo on or off
   - the pinning, and the core kept for the host
   - the storage and its fio flush latency
   - which points are close to the VM's size

## 8. Afterwards

- **Shut the VM down** (**Shutdown**).
- **Reboot the host** to restore hyperthreading, turbo and the frequency governor; or reverse
  the commands from part 2.
- **The results stay in the VM's `bench/results/matrix-*`.** Copy them out, or commit them from
  the VM.
