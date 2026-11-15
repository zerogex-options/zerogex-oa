# Giving the engines a box of their own

One server runs everything today: the website, the API, and the engines that
turn the market feed into numbers. This runbook moves the engines to a second
server without customers noticing. Paths assume the deploy roots
`~/zerogex-oa` and `~/zerogex-web`.

## The plan in one paragraph

Keep today's server as the **web box**: the website, the API, the auth
database and every scheduled job stay exactly where they are, so nothing
changes for DNS, certificates, Cloudflare, Stripe or logins. Launch one new
server, the **engine box**, the same size as today's, and move only the
always-on engines to it: the ThetaData terminal, ingestion, analytics, signals,
wall alerts and the backtest worker. The move happens on a **Saturday**, when
no market data flows anywhere (the engines' window is weekdays, and futures
are closed until Sunday 18:00 ET), so customers see no gap. Undoing it is the
same two commands in the other direction.

```
                      Cloudflare
                 zerogex.io   api.zerogex.io
                          │
   ┌──────────────────────▼───────────────┐    ┌──────────────────────────────┐
   │ WEB BOX (today's server, unchanged)  │    │ ENGINE BOX (new)             │
   │ nginx, website, auth DB, API,        │    │ ThetaData terminal,          │
   │ TradeWorkz, every scheduled job      │    │ ingestion, analytics,        │
   │ (timers), website billing/email jobs │    │ signals, wall alerts,        │
   │                                      │    │ backtest worker              │
   └──────────────────┬───────────────────┘    └───────────────┬──────────────┘
                      └──────── RDS PostgreSQL (shared) ───────┘
```

## Why this split

Every symbol you add costs the engines CPU (ingestion and analytics run one
process per symbol), and every customer you add costs the API CPU. Today they
share 8 cores, and the services' CPU caps already add up to more than that
(API 4, ingestion 1.5, analytics 1, signals 1, terminal 1, wall alerts 0.5,
plus the website and nginx uncapped). Splitting them means a busy afternoon of
traffic cannot slow the feed, and a new stock cannot slow the website. It also
gives each half its own dial: the engine box can be resized on any Saturday
with no customer impact, and the API gets the cores the engines leave behind.

Why the engines and not the website: the website holds local state (the auth
database is a file on that disk) and carries all the outside wiring, so moving
it is the risky direction. The engines keep nothing on disk but the ThetaData
terminal's folder, which you copy over; everything they write goes to RDS. The
API stays with the website because the website calls it on every page, and a
second network hop buys nothing yet.

Why one engine box and not several: ThetaData keeps **one session per
account**. A second terminal or ingestion anywhere knocks the first off the
feed, and they keep knocking each other off. So the engines always run in
exactly one place. `deploy/engine_box.sh` enforces that (see "Safety nets").

### What this does not fix

Three things limit how many symbols you can carry. The split fixes the first.

1. **The engines' CPU.** Fixed: the engine box is theirs alone, and it can grow.
2. **The database.** Every service shares RDS, and the 2026-11-06 baseline
   already averaged about 15 active sessions through the session. The split
   leaves the database's load exactly where it is (same engines, same limits).
   Measure it before the next stock goes in (step 1).
3. **ThetaData's 8 requests at a time per account.** Every symbol's ingestion
   process polls through the same 8. More servers do not change this. See
   "Later, only when the measurements say so".

## Safety nets built in

- **One owner at a time, recorded in the database.** `make engine-release`
  stops the engines on one box and writes "released" to the
  `engine_box_handoffs` table. `make engine-takeover` on the other box refuses
  unless the newest row says released. Taking over without releasing first is
  refused, not discovered at the open.
- **Marker files.** `/etc/zerogex/engines.disabled` on a box means "the
  engines do not run here". With it, a guard added to every engine unit
  refuses to start (so a habitual `make ingestion-restart` on the wrong box
  does nothing), `make services-restart` leaves the engines out, and the
  liveness watchdog stops paging for them. `/etc/zerogex/engine-box` marks the
  engine box: the API refuses to start there (TradeWorkz runs inside the API
  and would run twice), and `deploy.sh` installs only the base system there.
- **Dry runs.** Every engine command prints its plan and changes nothing
  unless you add `CONFIRM=yes`.
- **Nothing changes on today's server until you run a command.** Merging the
  branch adds files and one empty table. With no marker files, every make
  target, the watchdog and the capacity check behave exactly as they do now.

`make engine-status` shows all of it on either box, and exits non-zero if it
finds anything unsafe (engines running on a box marked off, scheduled jobs
enabled on the engine box, the database saying another box holds the engines).

## Timeline

| When | Step | Customer impact |
|---|---|---|
| A weekday, about 14:30 ET | 1. Baseline | none |
| Any weekday evening | 2. Merge and pull | none |
| Any day | 3-5. Launch and prepare the engine box | none: nothing starts there |
| A Saturday | 6. Cutover | none: no data flows |
| Saturday, Sunday 18:00 ET, Monday | 7. Check it | none |

Allow about an hour for steps 3-5 and 30 minutes on Saturday. Nothing here is
urgent; any step can wait a day.

---

## Step 1. Baseline (a weekday, during the session)

On today's server, at about the time of day you usually measure:

```bash
cd ~/zerogex-oa && make capacity-check LABEL=pre-split
```

Note the file name on the last line (under `~/monitoring/capacity/`). Also
note `free -m` and `uptime` once during the session.

Then look at the database, because it is the next limit. From your
workstation:

```bash
export AWS_REGION=us-east-2   # yours, if different
export RDS_ID=<the part of DB_HOST before the first dot>
aws rds describe-db-instances --db-instance-identifier $RDS_ID \
  --query 'DBInstances[0].[DBInstanceClass,MultiAZ,PerformanceInsightsEnabled,AvailabilityZone]' \
  --output table
```

In the RDS console, open the database's **Monitoring** tab and read
**CPUUtilization** for the session. Write down the instance class, whether it
is Multi-AZ, and the afternoon CPU. If Performance Insights is off, turn it on
(Modify, Performance Insights, the free 7-day retention, Apply immediately; it
needs no restart): after the move it shows the database's load per engine.

## Step 2. Merge and pull (a weekday evening, no restart)

Merge `claude/cool-babbage-wj8svd` into `release` in zerogex-oa. There is
nothing to merge in zerogex-web. Then, on today's server:

```bash
cd ~/zerogex-oa && make pull
```

`make pull` applies the schema, which adds the empty `engine_box_handoffs`
table. Nothing restarts. The running pieces that read changed files (the
liveness watchdog's script every minute, and the Makefile when a timer calls
it) behave exactly as before while no marker file exists. Check:

```bash
make engine-status
```

You want `Role: ONE BOX`, the engines that run today showing `enabled
active` (one you never installed, such as the backtest worker on some boxes,
shows `no`, and stays off on the engine box too), and `No unsafe state found.`

## Step 3. Launch the engine box (any day)

From your workstation. First read today's server's settings:

```bash
export WEB_ID=<today's instance id, EC2 console → Instances>
aws ec2 describe-instances --instance-ids $WEB_ID \
  --query 'Reservations[0].Instances[0].{type:InstanceType,ami:ImageId,subnet:SubnetId,vpc:VpcId,az:Placement.AvailabilityZone,key:KeyName}' \
  --output table
```

**A security group that lets in only your SSH.** The engine box takes no
traffic from anyone:

```bash
export VPC=<vpc from above>
ENGINE_SG=$(aws ec2 create-security-group --group-name zerogex-engine \
  --description "ZeroGEX engine box: SSH from my IP only" --vpc-id $VPC \
  --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id $ENGINE_SG --protocol tcp --port 22 \
  --cidr "$(curl -s https://checkip.amazonaws.com)/32"
```

**Let the database accept it.** This only adds a rule; today's server keeps
its own:

```bash
RDS_SG=$(aws rds describe-db-instances --db-instance-identifier $RDS_ID \
  --query 'DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId' --output text)
aws ec2 authorize-security-group-ingress --group-id $RDS_SG --protocol tcp --port 5432 \
  --source-group $ENGINE_SG
```

**A role that can read CloudWatch (recommended).** `make capacity-check`
reads the database's CPU from CloudWatch with the box's own credentials, and
today's server has none, so that gate has always been blind. In the IAM
console: Roles, Create role, trusted entity **AWS service: EC2**, attach
**CloudWatchReadOnlyAccess**, name it `zerogex-engine`, Create. (The console
makes the instance profile of the same name for you.) If `.env` says
`DB_PASSWORD_PROVIDER=aws_secrets_manager`, also give this role read access to
that secret (`secretsmanager:GetSecretValue` on `DB_SECRET_NAME`), as today's
server's role has; with the documented `pgpass`, nothing more is needed.

**Launch it** in the same subnet as today's server, with the same instance
type, key and Ubuntu release (check the release with `lsb_release -d` on
today's server):

```bash
ENGINE_ID=$(aws ec2 run-instances --image-id <ami from above> \
  --instance-type <type from above> --subnet-id <subnet from above> \
  --key-name <key from above> --security-group-ids $ENGINE_SG \
  --iam-instance-profile Name=zerogex-engine --associate-public-ip-address \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=50,VolumeType=gp3,Encrypted=true}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=zerogex-engine}]' \
  --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-status-ok --instance-ids $ENGINE_ID
aws ec2 describe-instances --instance-ids $ENGINE_ID \
  --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text
```

If AWS says that AMI is no longer available, use the current one for the same
Ubuntu release (24.04 shown):

```bash
aws ssm get-parameters \
  --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameters[0].Value' --output text
```

It needs no Elastic IP: nothing points at it. If you ever stop and start it,
its public address changes, and only your SSH cares.

## Step 4. Give it the code, the secrets and the terminal (any day)

From your workstation, so the two servers never need keys to each other:

```bash
export WEB=ubuntu@<today's server, as you ssh to it now>
export ENGINE=ubuntu@<engine box public IP>
ssh $ENGINE 'sudo hostnamectl set-hostname zerogex-engine && echo "127.0.1.1 zerogex-engine" | sudo tee -a /etc/hosts >/dev/null && sudo apt-get update && sudo apt-get install -y git make'
```

The name is only so alerts and prompts say which box they came from.

Clone the repo the way you did on today's server (same deploy key or token),
on `release`:

```bash
ssh $ENGINE 'git clone <your zerogex-oa repo url> ~/zerogex-oa && cd ~/zerogex-oa && git checkout release'
```

Copy `.env`, the Theta Terminal folder (jar, `creds.txt`, its config) and the
alert settings:

```bash
ssh $WEB 'cat ~/zerogex-oa/.env' | ssh $ENGINE 'umask 077 && cat > ~/zerogex-oa/.env'
ssh $WEB 'tar -C ~ -czf - thetaterminal' | ssh $ENGINE 'tar -C ~ -xzf -'
ssh $WEB 'sudo cat /etc/zerogex/alert.env' | ssh $ENGINE 'sudo install -d -o root -g ubuntu -m 0750 /etc/zerogex && sudo tee /etc/zerogex/alert.env >/dev/null && sudo chown root:ubuntu /etc/zerogex/alert.env && sudo chmod 0640 /etc/zerogex/alert.env'
```

(If the last one says there is no `alert.env`, alerting was never set up on
today's server; skip it.)

Install the same Java major version the terminal runs on today:

```bash
ssh $WEB 'java -version'
ssh $ENGINE 'sudo apt-get install -y openjdk-21-jre-headless'   # match the major above
```

Nothing copied here starts anything.

## Step 5. Prepare the engine box (any day; nothing starts)

On the engine box:

```bash
cd ~/zerogex-oa
make engine-prepare
make engine-prepare CONFIRM=yes
```

The first prints the plan. The second marks the box (both markers, first, so
nothing can start), runs the base install (`deploy/deploy.sh`, which on the
engine box runs only steps 010 015 020 030 040 050: packages, the Postgres
client and `.pgpass`, the Python venv, the firewall), then installs the engine
units **without enabling or starting them**, and switches on the box's own
monitoring (system monitor, liveness watchdog, nightly log cleanup). It never
logs in to ThetaData and never writes to the database. The base install takes
several minutes; if it stops, fix what it printed and run the same command
again.

Then:

```bash
make engine-status
```

You want `Role: ENGINE BOX, engines switched OFF`, every engine `yes
disabled inactive yes`, the Theta Terminal jar, `creds.txt` and java present,
and `No unsafe state found.` Run `make engine-status` on today's server too:
the two **fingerprints** of the symbol lists must match. If they do not, copy
`.env` again (step 4).

Optional, if you want the engine box's capacity check to time the API too:
allow port 8000 from the engine box on today's server (its security group, and
`sudo ufw allow from <engine private IP> to any port 8000`), then set
`CAPACITY_CHECK_API_BASE=http://<web private IP>:8000` in the engine box's
`.env`. Without it the check skips the API timing there, which is fine: the
API's latency is now the web box's business.

## Step 6. Cutover (a Saturday)

Saturday, any time. Nothing flows: the engines sleep until Monday and futures
are closed until Sunday 18:00 ET. The website and API keep running throughout.

**On today's server (the web box):**

```bash
cd ~/zerogex-oa
make engine-release
make engine-release CONFIRM=yes
```

The second marks the box off, adds the guard to every engine unit, stops the
engines last-first, switches them off at boot, and records the release. You
want `every engine is stopped and off at boot` and `release recorded in the
database`. Then:

```bash
make engine-status
```

`Role: WEB BOX, engines switched OFF here`, `zerogex-oa-api ... active`, no
unsafe state.

**On the engine box:**

```bash
cd ~/zerogex-oa
make engine-takeover
make engine-takeover CONFIRM=yes
```

The first should say `the engines were released by <web box> at <time>`. The
second removes the off marker and starts, in order, the terminal, ingestion,
analytics, signals, wall alerts and anything else that ran on the web box,
checking each stays up, then records the takeover. If one does not come up it
stops there and tells you how to hand the engines back.

```bash
make engine-status
```

`Role: ENGINE BOX, engines switched ON`, and the newest handoff row is
`took_over by zerogex-engine`.

## Step 7. Check it

**Saturday, right after the cutover, on the engine box:**

```bash
make theta-entitlements
```

The terminal's startup block: logged in, with your subscription line.

```bash
make feed-probe PROVIDER=thetadata UNDERLYING=SPY
```

One chain fetched through the new box's terminal: proof the credentials,
network and entitlement work, two days before you need them. Weekends only:
the probe logs in to ThetaData by itself, which on a weekday would bump
ingestion's session.

```bash
make alert-template-test
```

A test alert should reach you from the engine box. It names the normalizer
health check; that is only the test's stand-in unit.

**Sunday after 18:00 ET** (futures reopen): ES and NQ should be moving on the
site, and on either box

```bash
make ingestion-freshness-healthcheck
```

exits 0.

**Monday pre-market and at the open:** the site's live prices move; on the
engine box `make services-health` shows ingestion, analytics and signals
healthy.

**Monday about 14:30 ET**, the first comparison. Copy the baseline over, then
run the check on the engine box:

```bash
ssh $WEB 'cat ~/monitoring/capacity/<pre-split file>' | ssh $ENGINE 'mkdir -p ~/monitoring/capacity && cat > ~/monitoring/capacity/<pre-split file>'
```

```bash
cd ~/zerogex-oa && make capacity-check LABEL=engine-baseline BASELINE=~/monitoring/capacity/<pre-split file>
```

Expect the same engine numbers or better (no API competing for cores), the
API line skipped, and now the database CPU from CloudWatch. (If that line says
CloudWatch returned no datapoints, `AWS_REGION` in `.env` is not the
database's region: fix it on the web box and copy `.env` over.) This run is
the baseline for the next stock. On the web box, `uptime` and `free -m` should show
the room the engines left.

Leave the engines' unit files and the terminal folder on the web box. They
are what makes the rollback two commands.

## Rollback

Any time, in either direction, the same two commands:

```bash
# on the engine box
make engine-release CONFIRM=yes
```

```bash
# on the web box
make engine-takeover CONFIRM=yes
```

On a weekend this is invisible. During a session the live feed gaps for a
minute or two while the terminal logs in, the same as a restart.

If the engine box has died and cannot release, first make sure it is really
stopped (EC2 console: stopped or terminated), then on the web box:

```bash
make engine-takeover CONFIRM=yes FORCE=yes
```

Never use `FORCE=yes` while the other box might still be running its
terminal: the two would knock each other off the feed.

## After the move: what changes day to day

- **Deploying backend code.** `make pull` on the web box first: it applies any
  schema change, once, before anything restarts. Then `make pull` on the
  engine box, which skips the schema (the web box did it). Then
  `make services-restart` on each box: on the engine box it restarts
  ingestion, analytics and signals, one at a time and health-checked; on the
  web box, only the API. `make wall-alerts-restart` on the engine box after a
  change under `src/alerts/`, as before.
- **`.env` has one source: the web box.** Make every change there, then copy
  the whole file to the engine box and restart what reads it on each box:

  ```bash
  ssh $WEB 'cat ~/zerogex-oa/.env' | ssh $ENGINE 'umask 077 && cat > ~/zerogex-oa/.env'
  ```

  Both boxes need the same symbol lists: the engines ingest and model from
  them, and the web box's API and scheduled jobs read them too. `make
  engine-status` prints a fingerprint of every symbol list; they must match.
- **Adding a stock** (`mag7_rollout.md`): `make stock-env-add` on the web box,
  copy `.env` to the engine box, `make services-restart-ingest-analytics` on
  the engine box (the web box now refuses it), `sudo systemctl restart
  zerogex-oa-wall-alerts` on the engine box if step 1 named it, and the API
  restart on the web box if step 1 named it. `make capacity-check` runs on the
  engine box.
- **Scheduled jobs** all stay on the web box, including the tweet jobs (they
  render images with the website's code). The Sunday 02:30 ET full vacuum now
  stops and restarts only the API there; the engines are idle at that hour.
- **Reboots.** The engine box reboots itself at 03:00 ET when a security update
  needs it, as today's server does, and its engines come back on their own.
- **Resizing the engine box** is a Saturday job with no customer impact: stop
  it, change the instance type, start it. The engines come back on their own.
- **Known gap, can wait:** the website's Admin, Monitoring, Backend page reads
  the monitor file on the web box, so after the move it shows the web box and
  the API only; its ingestion, analytics and signals counts read zero. Use
  `make system-monitor-show` and `make services-health` on the engine box. A
  fix is a small follow-up.

## Later, only when the measurements say so

**The engines need more room** (capacity check: analytics overruns or option
rows getting older). The engine box has about 3 cores the engines' caps do not
use yet. Raising analytics' cap costs the database nothing extra (it still
reads once a minute per symbol); raising ingestion's writes option rows more
often, so check the database first. Each is a
unit-file change installed with `make service-limits-install` (ask me for the
numbers). Past that, resize the engine box on a Saturday.

**The database is busy** (capacity check: database WARN, or CPU averaging over
about 60% through the afternoon). Do not add stocks while it warns. In order:

1. If it is single-AZ, make it **Multi-AZ** first, on a weekend (Modify,
   Multi-AZ, Apply immediately). There is no downtime: AWS builds a standby in
   the background, and disk is a little slower while it copies. It also makes
   the next step a short failover instead of a reboot.
2. Then change the **instance class** on a Saturday. With Multi-AZ that is a
   failover of a minute or two: database connections drop and reconnect, the
   website stays up (logins are local), and API pages may error or show cached
   data for that minute. No market data flows on a Saturday.

**ThetaData's 8 at a time** (capacity check: thetadata WARN, failed price or
chain polls). Before going much beyond the MAG 7: ask ThetaData whether a plan
raises the limit and what it costs, and ask me for the setting that lets
stocks poll their chains every 15 to 30 seconds instead of 5. Nothing
downstream reads chains more often than once a minute.

**The API needs more room** (the web box's own capacity check warns on the
API, or the website slows at the open). First, more API workers: the cores are
free now (ask me). After that, the API on a box of its own. That needs one code
change first: the TradeWorkz scheduler inside the API holds a per-host lock,
so two API boxes would run it twice.

## Cost

One more server the size of today's: roughly double the EC2 line. Same subnet
as the web box keeps traffic between them and the database free, if the
database is in the same availability zone (step 1 printed it). Multi-AZ, if
you take it later, roughly doubles the database instance's cost.
