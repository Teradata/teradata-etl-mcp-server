# SSH Setup — Bidirectional (MCP Client ↔ Airflow)

The Teradata ETL MCP Extension requires **bidirectional SSH** between the MCP client machine and the Airflow server for two purposes:

| Direction | Purpose | When Needed |
|-----------|---------|-------------|
| MCP Client → Airflow Server | Deploy generated DAG files via SFTP | Required for DAG deployment |
| Airflow Server → MCP Client | Execute TdLoad/TPT commands remotely via SSH | When using TdLoad or TPT operators |

```
MCP Client Machine                          Airflow Server (Linux)
+---------------------+                    +---------------------+
| - MCP Server        | --- SSH/SFTP ----> | - /opt/airflow/dags |
| - TTU (tpt,         |   DAG deployment   | - Airflow Scheduler |
|   tdload)           |                    | - Airflow Workers   |
| - SSH Server        | <--- SSH --------- |                     |
|   (for runtime)     |  TdLoad exec       |                     |
+---------------------+                    +---------------------+
```

> **Platform note**: The MCP client can be **Windows** or **Linux/macOS**. The Airflow server is typically **Linux**. Instructions below cover all combinations.

Use **Ed25519** keys (faster and more secure than RSA).

---

## Step 1: Install SSH Client on MCP Client

**Windows:**
```powershell
# OpenSSH client is pre-installed on Windows 10/11. Verify:
ssh -V

# If not available, install:
Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0
```

**Linux/macOS:**
```bash
# Usually pre-installed. Verify:
ssh -V

# If not available:
# Ubuntu/Debian
sudo apt install openssh-client
# macOS -- built-in, no action needed
```

---

## Step 2: Install SSH Server on MCP Client (for Runtime SSH)

Airflow needs to SSH **back** to the MCP client to execute TdLoad/TPT commands. This requires an SSH server running on the MCP client.

**Windows (run as Administrator):**
```powershell
# Check if OpenSSH Server is already installed
Get-Service sshd -ErrorAction SilentlyContinue

# If the service exists (even if Stopped), skip Add-WindowsCapability and just start it:
#   Start-Service sshd
# Only run the line below if Get-Service sshd returns nothing:
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0

# Start and auto-enable the service
Start-Service sshd
Set-Service -Name sshd -StartupType Automatic

# Verify it's running
Get-Service sshd

# Allow SSH through Windows Firewall (if not already)
New-NetFirewallRule -Name "OpenSSH-Server" -DisplayName "OpenSSH Server (sshd)" `
    -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22
```

> **Note:** On many Windows 10/11 and Server 2019+ machines, OpenSSH Server is already installed but stopped. Run `Get-Service sshd` first — if it returns `Stopped`, only `Start-Service sshd` is needed.

**Linux:**
```bash
# Ubuntu/Debian
sudo apt install openssh-server
sudo systemctl enable --now ssh

# RHEL/CentOS
sudo yum install openssh-server
sudo systemctl enable --now sshd

# Verify
sudo systemctl status ssh    # or sshd
```

**macOS:**
```bash
# Enable via System Settings > General > Sharing > Remote Login
# Or via command line:
sudo systemsetup -setremotelogin on
```

---

## Step 3: Generate SSH Keys — Direction 1 (MCP Client → Airflow)

Generate a key pair **on the MCP client** for deploying DAG files to Airflow.

**Windows (PowerShell):**
```powershell
# Create .ssh directory if it doesn't exist
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.ssh"

# Generate Ed25519 key pair (-N "" sets an empty passphrase non-interactively)
ssh-keygen -t ed25519 -f "$env:USERPROFILE\.ssh\id_ed25519_airflow" -C "mcp-to-airflow" -N ""

# Copy public key to Airflow server
# Note: 'type | ssh' can silently fail on Windows — use Get-Content + variable instead:
$pubKey = (Get-Content "$env:USERPROFILE\.ssh\id_ed25519_airflow.pub" -Raw).Trim()
ssh airflow@<airflow-host> "mkdir -p ~/.ssh && chmod 700 ~/.ssh && echo '$pubKey' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"

# Verify connectivity
ssh -i "$env:USERPROFILE\.ssh\id_ed25519_airflow" -o BatchMode=yes airflow@<airflow-host> "echo 'DAG deployment SSH OK'"
```

> **Windows gotcha:** `type file.pub | ssh ...` can silently succeed (exit code 0) but fail to write the key on the remote side. Always use the `Get-Content` + variable method above.

**Linux/macOS:**
```bash
# Generate Ed25519 key pair
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_airflow -C "mcp-to-airflow"
# Press Enter for no passphrase (recommended for automated deployment)

# Copy public key to Airflow server
ssh-copy-id -i ~/.ssh/id_ed25519_airflow.pub airflow@<airflow-host>

# Verify connectivity
ssh -i ~/.ssh/id_ed25519_airflow airflow@<airflow-host> "echo 'DAG deployment SSH OK'"
```

Set these in `.env` on the MCP client:
```bash
AIRFLOW_REMOTE_HOST=<airflow-host>
AIRFLOW_REMOTE_USER=airflow
# Windows: use full path, e.g., C:\Users\YourUser\.ssh\id_ed25519_airflow
# Linux/macOS: ~/.ssh/id_ed25519_airflow
AIRFLOW_REMOTE_SSH_KEY=~/.ssh/id_ed25519_airflow
# AIRFLOW_REMOTE_PASSWORD=          # only if not using key-based auth
```

---

## Step 4: Generate SSH Keys — Direction 2 (Airflow → MCP Client)

Generate a key pair **on the Airflow server** for runtime TdLoad/TPT execution on the MCP client.

**On Airflow server (Linux):**
```bash
# Switch to the airflow user (the user that runs DAG tasks)
sudo su - airflow

# First, check if a key already exists — reuse it to avoid key sprawl:
ls ~/.ssh/
# Look for any existing id_ed25519_* keys. If one exists for this purpose, skip generation.

# Only generate a new key if none exists:
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_mcp -C "airflow-to-mcp"

# Display the public key (you'll need this for the next step)
cat ~/.ssh/id_ed25519_mcp.pub
```

> **Tip:** If the Airflow server already has a key (e.g. `id_ed25519_test`), use `cat ~/.ssh/<existing_key>.pub` and reuse it. Update `MCP_CLIENT_SSH_KEY_PATH` in `.env` to match the actual key path found.

**Authorize the key on MCP client — Windows:**
```powershell
# Create .ssh directory if it doesn't exist
mkdir -Force "$env:USERPROFILE\.ssh"

# Append the public key from Airflow server to authorized_keys
# Copy the output of 'cat ~/.ssh/id_ed25519_mcp.pub' from Airflow and paste below:
Add-Content "$env:USERPROFILE\.ssh\authorized_keys" "ssh-ed25519 AAAA...paste-key-here... airflow-to-mcp"

# For Windows OpenSSH, admin users use a different authorized_keys location:
# If your user is in the Administrators group, use ONLY this file (not ~/.ssh/authorized_keys):
Set-Content "C:\ProgramData\ssh\administrators_authorized_keys" "ssh-ed25519 AAAA...paste-key-here... airflow-to-mcp" -Encoding ascii

# Fix permissions on administrators_authorized_keys (required by Windows OpenSSH)
# Without this icacls fix, key auth will be silently rejected even if the key is correct:
icacls "C:\ProgramData\ssh\administrators_authorized_keys" /inheritance:r /grant "SYSTEM:(F)" /grant "Administrators:(F)"
```

**Authorize the key on MCP client — Linux/macOS:**
```bash
# Simplest method: ssh-copy-id from the Airflow server
# On Airflow server:
ssh-copy-id -i ~/.ssh/id_ed25519_mcp.pub <your-user>@<mcp-client-host>

# Or manually on MCP client:
mkdir -p ~/.ssh && chmod 700 ~/.ssh
# Append the public key
echo "ssh-ed25519 AAAA...paste-key-here... airflow-to-mcp" >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

**Verify from Airflow server:**
```bash
# Test SSH to MCP client
ssh -i ~/.ssh/id_ed25519_mcp -o BatchMode=yes <your-user>@<mcp-client-host> "echo 'Runtime SSH OK'"

# Verify TTU is available on MCP client
ssh -i ~/.ssh/id_ed25519_mcp <your-user>@<mcp-client-host> "which tbuild || which tdload || where tbuild"
```

Set these in `.env` on the MCP client:
```bash
MCP_CLIENT_SSH_HOST=<mcp-client-host>           # your machine's IP or hostname
MCP_CLIENT_SSH_USER=<your-user>                  # your username on this machine
MCP_CLIENT_SSH_PORT=22
# Use the actual path of the key on the Airflow server — verify with 'ls ~/.ssh/' first
MCP_CLIENT_SSH_KEY_PATH=~/.ssh/id_ed25519_mcp   # path on the Airflow server
```

> **Common misconfiguration:** `MCP_CLIENT_SSH_KEY_PATH` must match the exact path of the private key **on the Airflow server**, not on the Windows machine. Always confirm with `ls ~/.ssh/` on the Airflow server after setup.

---

## Step 5: File Permissions

**Linux/macOS (both machines):**
```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519_*           # private keys
chmod 644 ~/.ssh/id_ed25519_*.pub       # public keys
chmod 600 ~/.ssh/authorized_keys
```

**Windows (MCP client):**
```powershell
# Windows OpenSSH enforces permissions via ACLs. Private keys should only be
# readable by the owner. Right-click the key file > Properties > Security:
#   - Remove all inherited entries and keep only your account
#   - Or use icacls:
icacls "$env:USERPROFILE\.ssh\id_ed25519_airflow" /inheritance:r /grant "${env:USERNAME}:(R)"
```

---

## Optional: SSH Config File

Add entries for convenience (avoids typing full paths each time).

**On MCP client (`~/.ssh/config` or `%USERPROFILE%\.ssh\config`):**
```
Host airflow
    HostName <airflow-host>
    User airflow
    IdentityFile ~/.ssh/id_ed25519_airflow
    Port 22
    StrictHostKeyChecking accept-new
```

**On Airflow server (`~/.ssh/config`):**
```
Host mcp-client
    HostName <mcp-client-host>
    User <your-user>
    IdentityFile ~/.ssh/id_ed25519_mcp
    Port 22
    StrictHostKeyChecking accept-new
```

---

## Verification Checklist

| Check | Command (run from) | Expected Output |
|-------|-------------------|----------------|
| DAG deployment SSH | MCP client: `ssh airflow@<airflow-host> "ls /opt/airflow/dags/"` | Lists DAG directory |
| Runtime SSH | Airflow: `ssh <user>@<mcp-client-host> "echo OK"` | `OK` |
| TTU available | Airflow: `ssh <user>@<mcp-client-host> "tbuild -v"` | TPT version info |

---

## SSH Host-Key Verification

All MCP tool actions that open an SSH/SFTP connection accept a
`strict_host_key_checking` parameter. The full set:

| Tool | Action | Connection use |
|---|---|---|
| `pipeline_deploy` | `deploy_dags` | SFTP transfer of generated DAG files to the Airflow server |
| `pipeline_deploy` | `deploy_complete` | Full pipeline transfer (DAG + TPT + dbt + CSV) |
| `pipeline_control` | `update_schedule` (when `auto_deploy=True`) | SFTP fetch of the remote DAG, edit, re-upload |
| `pipeline_control` | `delete` | Remote DAG file removal |

**Default: `False`** — suitable for development and single-machine setups. For shared or production environments, upgrade to strict mode (see below).

When disabled, the extension logs a `WARNING` on every SSH connection:

```
WARNING  SSH host-key verification is DISABLED for deploy_dags
(strict_host_key_checking=False). This exposes you to MITM attacks.
```

### Upgrading to Strict Mode (recommended for shared/production setups)

1. Pre-populate the Airflow server's host key in the MCP client user's `known_hosts`:

   ```bash
   # Git Bash / PowerShell / Linux / macOS
   ssh-keyscan -t ed25519,rsa <airflow-host> >> ~/.ssh/known_hosts
   ```

   ```cmd
   :: Windows cmd.exe
   ssh-keyscan -t ed25519,rsa <airflow-host> >> %USERPROFILE%\.ssh\known_hosts
   ```

2. When deploying DAGs, tell Copilot to use strict host-key checking:

   ```
   Deploy the DAG to Airflow with strict host-key checking enabled
   ```

3. Verify no `SSH host-key verification is DISABLED` warning appears in the extension logs after the deployment.
