# Replacement Instance Runbook

What to do when you need to bring up a fresh EC2 instance to replace the
current one — whether for instance type changes, AMI updates, suspected
compromise, or recovery from a catastrophic failure.

**Estimated duration**: 25–40 min total. ~5–10 min of API downtime during
the EIP cutover (Phase 2).

---

## When to use this

| Scenario | Use this runbook? |
|---|---|
| Resize instance type (e.g. `m5.large` → `m5.xlarge`) | No — just stop, modify, start. EIP follows. |
| Suspected instance compromise | **Yes** — replace, don't reuse. |
| Root volume corruption | **Yes** |
| Rolling out a major OS-level change | **Yes** (lets you A/B before cutover) |
| Recovering from a deleted/terminated instance | **Yes**, but skip Phase 2's "stop old services" step |
| Routine deploy (just code changes) | No — `git pull && systemctl restart` on the existing instance |

---

## Assumed setup

These should already be true from the initial setup. Verify in Phase 0
if unsure.

- One Elastic IP (call its allocation ID `EIP_ALLOC`) is currently
  associated with the live instance.
- DNS A record for `$OA_API_DOMAIN` points at the EIP.
- A launch template exists with the right AMI, instance type,
  security group, IAM role, and key pair. **It does NOT auto-associate
  the EIP at launch** — we attach it manually at cutover so the new
  instance can be deployed without disturbing the live one.
- `.env` values are stored somewhere recoverable (password manager,
  SSM Parameter Store, or your own offline copy). Anything in `.env`
  that can't be reproduced from memory should be backed up.
- All durable state lives in RDS, not on the instance. (If you've
  added local state, e.g. monitoring logs in `/data`, plan to either
  drain it before terminating the old box or accept the loss.)

---

## Sibling repo: `zerogex-web`

The Next.js frontend at `https://zerogex.io` and its nginx config live in
a separate repo (`zerogex-web`). That repo owns
`/etc/nginx/sites-available/zerogex-web`, which contains the
`location /api/` block proxying user-facing API traffic to FastAPI on
port 8000. **Most production API traffic flows through `zerogex-web`'s
nginx config, not the `api.zerogex.io` config in this repo.**

This creates a cross-repo nginx dependency:

- **This repo** (`zerogex-oa`, `deploy/steps/120.nginx_api`) defines the
  shared cache zone `zerogex_cache` at
  `/etc/nginx/conf.d/zerogex-cache-zone.conf`, and bumps
  `worker_connections`, `LimitNOFILE`, the per-IP rate limit, and proxy
  timeouts in `nginx.conf`.
- **`zerogex-web`** must reference that zone in its `location /api/`
  block (`proxy_cache zerogex_cache;` plus the matching `proxy_cache_*`
  directives, the looser `burst=1000` rate limit, and the 30s/120s/120s
  proxy timeouts).

**Both halves must be deployed for the API to survive peak trading
load.** Without `zerogex-web`'s cache directives, all frontend `/api/*`
calls bypass the cache, hammer FastAPI directly, and the box CPU-saturates
within minutes of the afternoon session opening (this caused a real
incident on 2026-05-05).

When replacing the instance, after this repo's `deploy.sh` finishes:

1. Clone and deploy `zerogex-web` on the new instance — its nginx config
   must include the cache directives referenced above.
2. `sudo nginx -t && sudo systemctl reload nginx`.
3. Confirm the cache is actually populating (see Phase 3).

---

## Phase 0 — Decide and prepare (5 min)

```bash
# Set these once at the top of your shell
export AWS_REGION=us-east-2
export OLD_INSTANCE_ID=i-xxxxxxxxxxxxxxxxx
export EIP_ALLOC=eipalloc-xxxxxxxxxxxxxxxxx
export LAUNCH_TEMPLATE_NAME=zerogex-oa
export DOMAIN=api.zerogex.io
```

**Confirm the EIP and DNS**:

```bash
aws ec2 describe-addresses --allocation-ids $EIP_ALLOC \
  --query 'Addresses[0].[PublicIp,InstanceId,AssociationId]' --output table
dig +short $DOMAIN A
# Both IPs should match.  AssociationId is needed for the swap later.
```

Stash the AssociationId:
```bash
export OLD_ASSOC=$(aws ec2 describe-addresses --allocation-ids $EIP_ALLOC \
  --query 'Addresses[0].AssociationId' --output text)
```

**Take an RDS snapshot** (cheap insurance — RDS is the only durable state):
```bash
aws rds create-db-snapshot \
  --db-instance-identifier <your-rds-id> \
  --db-snapshot-identifier "pre-replace-$(date +%Y%m%d-%H%M)"
```

**Have `.env` ready**: either the file itself, or all values you'll need
to fill it in.

---

## Phase 1 — Launch new instance and deploy (15–25 min)

The new instance comes up with its own auto-assigned public IP. The live
instance is still serving traffic via the EIP, untouched.

### 1.1 Launch from template

```bash
NEW_INSTANCE_ID=$(aws ec2 run-instances \
  --launch-template "LaunchTemplateName=$LAUNCH_TEMPLATE_NAME,Version=\$Latest" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=zerogex-oa-new},{Key=Role,Value=replacement}]' \
  --query 'Instances[0].InstanceId' --output text)
echo "new instance: $NEW_INSTANCE_ID"

aws ec2 wait instance-running --instance-ids $NEW_INSTANCE_ID
aws ec2 wait instance-status-ok --instance-ids $NEW_INSTANCE_ID

NEW_TEMP_IP=$(aws ec2 describe-instances --instance-ids $NEW_INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "ssh to: ubuntu@$NEW_TEMP_IP"

# Sanity check.  If the launch template's network interface block does
# not set AssociatePublicIpAddress=true (and the subnet's
# MapPublicIpOnLaunch is false), the new instance comes up with no
# public IP and we can't SSH to it.  Fix the launch template before
# proceeding — relying on the EIP cutover to grant SSH access creates
# a chicken-and-egg with Phase 1.2.
if [ "$NEW_TEMP_IP" = "None" ] || [ -z "$NEW_TEMP_IP" ]; then
    echo "✗ new instance has no public IP — cannot SSH"
    echo "  Update the launch template's network interface to set"
    echo "  AssociatePublicIpAddress=true, then re-launch."
    exit 1
fi
```

### 1.2 Deploy on the new instance — everything except SSL

SSH in via the temporary IP and bring the system up. Step 130 (SSL) will
fail because DNS still points at the old instance — that's expected and
desired. We'll finish SSL after the EIP swap in Phase 2.

```bash
ssh -o StrictHostKeyChecking=accept-new ubuntu@$NEW_TEMP_IP
```

On the new instance:

```bash
git clone <your-repo-url> ~/zerogex-oa
cd ~/zerogex-oa
chmod +x deploy/deploy.sh deploy/steps/*

# Restore your .env (scp from workstation, or pull from SSM, or paste)
$EDITOR .env
chmod 600 .env

# Run the deploy.  Will fail at step 130 with a DNS-mismatch error — OK.
./deploy/deploy.sh
```

When step 130 fails with `DNS mismatch — $DOMAIN resolves to <eip>, this
instance is at <new_temp_ip>`, **stop here**. The error message itself is
the marker that Phase 1 is done.

### 1.3 Pre-cutover smoke test

Before disturbing the live instance, sanity-check the new one. Still
SSHed into the new instance:

```bash
# All non-SSL services should be running
systemctl --no-pager --no-legend list-units 'zerogex-oa-*' --state=running
# Expect: api, ingestion, signals, analytics

# API responds locally
API_KEY=$(awk -F= '/^API_KEY=/{print $2; exit}' ~/zerogex-oa/.env | tr -d '"'"'")
curl -fsS -H "X-API-Key: $API_KEY" http://127.0.0.1:8000/api/health

# nginx is listening on 80
curl -fsS -H "Host: $OA_API_DOMAIN" http://127.0.0.1/api/health
```

If any of those fail, debug here — you have unlimited time, the live
instance is unaffected.

---

## Phase 2 — Cutover (5–10 min, this is the downtime window)

This is the only part with user-visible impact. Move quickly and in
order. **Have two terminals open**: one SSHed into the new instance,
one with AWS CLI for the swap.

### 2.1 Stop services on the OLD instance

Avoids double-writes to RDS during the swap window. From your local
shell or another SSH session to the old box:

```bash
ssh ubuntu@$DOMAIN   # still resolves to old instance
sudo systemctl stop zerogex-oa-signals zerogex-oa-analytics zerogex-oa-ingestion zerogex-oa-api
exit
```

**Downtime starts now.**

### 2.2 Swap the EIP

```bash
# Disassociate from old
aws ec2 disassociate-address --association-id $OLD_ASSOC

# Associate with new (this releases the new instance's auto-assigned IP)
NEW_ASSOC=$(aws ec2 associate-address \
  --instance-id $NEW_INSTANCE_ID \
  --allocation-id $EIP_ALLOC \
  --query 'AssociationId' --output text)
echo "new association: $NEW_ASSOC"

# Confirm
aws ec2 describe-addresses --allocation-ids $EIP_ALLOC \
  --query 'Addresses[0].[PublicIp,InstanceId]' --output table
```

Note: your existing SSH session to the new instance via `$NEW_TEMP_IP`
will drop within ~30 s — that auto-assigned IP is released the moment
the EIP attaches. Reconnect via the EIP / DNS:

```bash
ssh ubuntu@$DOMAIN
```

DNS doesn't change during the cutover (the EIP itself moves between
instances), so resolver caches don't matter here. The actual lag is
**IMDS** on the new instance reflecting the freshly-attached EIP — the
public-ipv4 metadata endpoint can take ~30 s to update. If step 130's
DNS preflight reports a mismatch immediately after the swap, wait 30 s
and re-run.

### 2.3 Finish SSL on the new instance

```bash
cd ~/zerogex-oa
./deploy/deploy.sh --start-from 130
```

Step 130's DNS preflight should now confirm `$DOMAIN` resolves to this
instance's public IP, and certbot will issue the cert. **Downtime ends
once `nginx -t` passes and reload completes** (the redirect block makes
the site responsive on https immediately).

---

## Phase 3 — Verify (5 min)

Independent of the new instance, from your workstation:

```bash
# DNS still pointing where we expect
dig +short $DOMAIN A

# TLS is valid
curl -fsS -o /dev/null -w 'HTTP %{http_code} — TLS %{ssl_verify_result}\n' \
  https://$DOMAIN/api/health

# A real auth'd call goes through nginx → uvicorn → RDS round-trip
API_KEY="<your key>"
curl -fsS -H "X-API-Key: $API_KEY" "https://$DOMAIN/api/gex/summary?symbol=SPX" | jq .

# Cert chain verifies (returns non-zero on any chain or hostname error,
# unlike `openssl x509 -noout -dates` which just prints fields without
# checking anything)
openssl s_client -verify_return_error -servername $DOMAIN \
  -connect $DOMAIN:443 -CApath /etc/ssl/certs </dev/null >/dev/null 2>&1 \
  && echo "TLS chain verifies" || echo "TLS chain FAILED"
```

On the new instance:

```bash
# All services healthy
systemctl --no-pager status zerogex-oa-api zerogex-oa-ingestion \
  zerogex-oa-signals zerogex-oa-analytics --no-pager | grep -E 'Active:|Main PID'

# No steal time (m5 is non-burstable; should be 0.0)
top -b -n 1 | head -5

# Logs flowing without tracebacks
sudo journalctl -u zerogex-oa-api --since "5 minutes ago" --no-pager | tail -30
sudo journalctl -u zerogex-oa-ingestion --since "5 minutes ago" --no-pager | tail -30

# Cert renewal timer is enabled
systemctl status zerogex-cert-renew.timer --no-pager

# Response cache is populating (proves the zerogex-web /api/ block was
# also deployed with cache directives — see "Sibling repo" section).
# Should be > 0 within seconds of real traffic.
sudo find /var/cache/nginx/zerogex -type f | wc -l
```

If any of those look wrong, **roll back** (see below) before terminating
the old instance.

---

## Phase 4 — Decommission the old instance (after a soak period)

Don't terminate immediately. Leave the old instance **stopped** for
24–48 h as a fast rollback option (stopped instances don't bill for
compute, only for EBS).

```bash
aws ec2 stop-instances --instance-ids $OLD_INSTANCE_ID
aws ec2 wait instance-stopped --instance-ids $OLD_INSTANCE_ID
```

After the soak, terminate:

```bash
aws ec2 terminate-instances --instance-ids $OLD_INSTANCE_ID
```

Also untag the new one:

```bash
aws ec2 create-tags --resources $NEW_INSTANCE_ID \
  --tags Key=Name,Value=zerogex-oa Key=Role,Value=primary
aws ec2 delete-tags --resources $NEW_INSTANCE_ID \
  --tags Key=Role,Value=replacement
```

---

## Rollback

### Rollback before EIP swap (Phase 1 failed)

No user impact occurred. Just terminate the new instance and try again
or investigate:

```bash
aws ec2 terminate-instances --instance-ids $NEW_INSTANCE_ID
```

### Rollback after EIP swap (Phase 2/3 found a problem)

Move the EIP back to the old instance:

```bash
# If you've already moved to Phase 4 (old instance stopped), start it
# first — a stopped instance has no networking and the EIP can't
# reach it.
aws ec2 start-instances --instance-ids $OLD_INSTANCE_ID
aws ec2 wait instance-running --instance-ids $OLD_INSTANCE_ID
aws ec2 wait instance-status-ok --instance-ids $OLD_INSTANCE_ID

# Disassociate from new
aws ec2 disassociate-address --association-id $NEW_ASSOC

# Associate back with old
aws ec2 associate-address \
  --instance-id $OLD_INSTANCE_ID \
  --allocation-id $EIP_ALLOC

# Restart services on old
ssh ubuntu@$DOMAIN
sudo systemctl start zerogex-oa-ingestion zerogex-oa-analytics zerogex-oa-signals zerogex-oa-api
```

Total rollback window: 1–3 min.

---

## Common pitfalls

- **Cert request rate-limit (Let's Encrypt)**: production endpoint
  allows 5 duplicate cert issuances per `$DOMAIN` per week. If you're
  rehearsing this runbook repeatedly, use `--dry-run` or expect to wait.
- **`--start-from 130` runs ALL subsequent steps** (130, 200). That's
  fine — 200 is just validation — but be aware the `--start-from` flag
  is "start from", not "run only".
- **SSH host key changed warning** when reconnecting via DNS after the
  swap: `ssh-keygen -R $DOMAIN` to clear the cached key, then accept the
  new one. Verify the fingerprint via the EC2 console's "Get system log"
  if you want to be paranoid.
- **EIP charges**: AWS charges for an EIP that is *not* associated with
  a running instance. During the swap, the EIP is briefly unassociated;
  fine. But if you stop the new instance while the EIP is attached, you
  start paying the EIP idle fee.
- **ENI persistence**: if your launch template attaches a specific ENI
  (network interface) instead of creating a new one, only one instance
  can use it at a time. Don't structure the template that way unless
  you know why.
- **TradeStation refresh token reuse**: both old and new instances
  using the same refresh token is fine for a short window — the token
  rotates on use, and whoever used it last "wins". After cutover, the
  old instance's local token is stale; it would fail to refresh if you
  ever started its services again. That's expected.
- **CloudWatch agent / SSM agent state**: the new instance shows up as
  a new entry in CloudWatch and SSM. Old dashboards/alarms keyed by
  instance ID need to be repointed. Use tags-based alarms to avoid this.

---

## Quick reference

```bash
# Launch + wait
NEW_INSTANCE_ID=$(aws ec2 run-instances --launch-template "LaunchTemplateName=$LAUNCH_TEMPLATE_NAME,Version=\$Latest" --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-status-ok --instance-ids $NEW_INSTANCE_ID

# EIP swap (atomic-ish; ~5s gap)
aws ec2 disassociate-address --association-id $OLD_ASSOC
aws ec2 associate-address --instance-id $NEW_INSTANCE_ID --allocation-id $EIP_ALLOC

# Rollback
aws ec2 disassociate-address --association-id $NEW_ASSOC
aws ec2 associate-address --instance-id $OLD_INSTANCE_ID --allocation-id $EIP_ALLOC

# Decommission
aws ec2 stop-instances     --instance-ids $OLD_INSTANCE_ID   # 48h soak
aws ec2 terminate-instances --instance-ids $OLD_INSTANCE_ID  # final
```
