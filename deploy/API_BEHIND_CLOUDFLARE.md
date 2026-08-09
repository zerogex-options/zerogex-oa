# Putting `api.zerogex.io` behind Cloudflare

Goal: proxy the API through Cloudflare (orange-cloud) so the origin is hidden and
gets Cloudflare's DDoS/WAF protection — and so the origin `:443` can eventually
be locked to Cloudflare-only for **both** the website and the API (full origin
lockdown, the endgame of the mTLS work in the web repo).

Read this whole file before touching anything. The order matters, and one step
(WAF/bot exclusion) will break every programmatic API client if skipped.

## Who's affected

- **External API consumers only** — API-key holders, the NinjaTrader
  `NT8-ZeroGexMap` tool, anything hitting `https://api.zerogex.io`.
- **NOT the website.** The site's BFF talks to uvicorn at `127.0.0.1:8000`
  directly (`ZEROGEX_API_BASE_URL`), not through `api.zerogex.io`, so the site is
  unaffected by this change.

## Critical prerequisite — do NOT skip

`api.zerogex.io` serves **programmatic, non-browser** clients. Cloudflare's bot
and browser-integrity defenses will challenge those with a JS/managed challenge
and **break every API consumer** unless the hostname is excluded. Before flipping
DNS you MUST, in the Cloudflare dashboard:

- **Bot Fight Mode / Super Bot Fight Mode:** exclude `api.zerogex.io` (or leave
  it off for the zone). SBFM: add a **skip** rule for `Host eq "api.zerogex.io"`.
- **Browser Integrity Check:** off for `api.zerogex.io` (Configuration Rule).
- **Managed Challenge / WAF managed rules:** add a WAF **custom rule** at the top:
  `(http.host eq "api.zerogex.io")` → action **Skip** → skip *all* remaining
  custom rules, rate-limiting rules, managed rules, and Super Bot Fight Mode.

Verify after cutover with the curl in step 6 — a challenge shows up as an HTML
"Just a moment…"/`cf-mitigated` response instead of your JSON.

## Other Cloudflare settings

- **SSL/TLS mode: Full (strict).** The origin already serves a valid Let's
  Encrypt cert for `api.zerogex.io`, which Full (strict) accepts — so you can cut
  over with the *existing* cert and switch to a Cloudflare Origin Certificate
  later (step 7), with no cert-trust gap.
- **Cache:** Cloudflare does not cache JSON by default, but be explicit — add a
  **Cache Rule**: `http.host eq "api.zerogex.io"` → **Bypass cache**. (The origin
  already does its own 5 s micro-cache; you don't want CF caching per-credential
  API responses.)
- **Origin response timeout:** on Free/Pro/Business the edge times out at ~100 s
  and returns **524**. The origin's heaviest queries run well under that (~12–14 s
  worst case; nginx allows 120 s), so this is only a tail risk — watch for 524s
  after cutover and optimize any endpoint that approaches 100 s (or use an
  Enterprise plan for a longer timeout).

## Real client IP

Already handled on this co-hosted box: the web deploy writes
`/etc/nginx/conf.d/zerogex-realip.conf` (`set_real_ip_from <CF ranges>` +
`real_ip_header CF-Connecting-IP`) at **http scope**, so it applies to the
`api.zerogex.io` server block too. Once proxied, the API's `$remote_addr` and its
`zerogex_api_per_ip` rate limit key on the true client — no global-throttle bug.
The weekly `zerogex-web-refresh-cf-ranges.timer` keeps the ranges current.

> If the API is ever moved to its **own box** (no co-hosted web deploy),
> replicate that snippet + refresh timer there first — otherwise the API's rate
> limit collapses onto Cloudflare's edge IPs (the exact outage we just fixed).

## Rollout (staged, low-risk; do it off-hours)

**1. Lower DNS TTL** for `api.zerogex.io` to 60 s a little ahead of time, so a
rollback propagates fast.

**2. Cloudflare prep** (no traffic impact yet — DNS still grey-cloud):
SSL/TLS = Full (strict); the WAF/bot **skip** rule and Browser-Integrity-Check
off for the host; the cache-bypass rule. Double-check the skip rule is first.

**3. Flip DNS to proxied** (orange cloud) for `api.zerogex.io`.
Clients now terminate TLS at Cloudflare (trusted edge cert); CF→origin uses the
existing valid LE cert (Full strict accepts it).

**4. Verify it's serving through Cloudflare:**
```bash
curl -sI https://api.zerogex.io/api/health | grep -iE 'HTTP/|cf-ray|server'
# expect: HTTP/2 200, a cf-ray: header (proves it's via Cloudflare), server: cloudflare
```

**5. Verify real client IPs reach the origin** (not CF edges) — on the box:
```bash
sudo tail -n 50 /var/log/nginx/access.log | grep 'api/' | awk '{print $1}' | sort -u
# expect diverse real client IPs, NOT 104.16.x / 172.6x.x Cloudflare ranges
```

**6. Verify programmatic clients aren't challenged** (the WAF-exclusion check):
```bash
curl -s -H "X-API-Key: <a-real-key>" \
  "https://api.zerogex.io/api/gex/summary?symbol=SPY" | head -c 200; echo
# expect JSON. If you get an HTML "Just a moment…" page, the bot/WAF skip rule
# is wrong — fix step 2 before considering this done.
```
Also confirm `https://api.zerogex.io/docs` loads and a known external client
(e.g. the NinjaTrader tool) still works.

**7. Cert hygiene — before the LE cert expires (~90 d):** switch the origin to a
long-lived **Cloudflare Origin Certificate** so there's no renewal dependency
behind CF (Let's Encrypt HTTP-01 can't validate once proxied).
- Cloudflare → SSL/TLS → Origin Server → **Create Certificate** (hostname
  `api.zerogex.io` or `*.zerogex.io`). Save the cert + key on the box:
  ```bash
  sudo install -d -m 755 /etc/nginx/cloudflare
  sudo install -m 644 origin.pem /etc/nginx/cloudflare/api.zerogex.io.pem
  sudo install -m 600 origin.key /etc/nginx/cloudflare/api.zerogex.io.key
  ```
- Point the deploy at it and skip certbot (persist these in the OA deploy env):
  ```bash
  export SKIP_CERTBOT=1
  export OA_TLS_CERT=/etc/nginx/cloudflare/api.zerogex.io.pem
  export OA_TLS_KEY=/etc/nginx/cloudflare/api.zerogex.io.key
  ./deploy/deploy.sh --start-from 130      # writes nginx with the origin cert, no certbot
  ```
- Disable the now-unusable LE renewal for this host:
  ```bash
  sudo systemctl disable --now zerogex-cert-renew.timer
  ```
  (With `SKIP_CERTBOT=1` persisted, future deploys won't re-enable it or run the
  DNS preflight, which would otherwise fail now that the host resolves to CF.)

## Rollback

Flip `api.zerogex.io` DNS back to **DNS-only** (grey cloud). Traffic goes direct
to the origin again, which still serves the trusted LE cert — so external clients
keep working. (If you already did step 7 and removed LE, re-issue an LE cert
first, or roll back only to before step 7.) Because you lowered the TTL in step 1,
this propagates in ~a minute.

## After this: full origin lockdown

Once the API is proxied and verified, both `zerogex.io` and `api.zerogex.io` are
behind Cloudflare, so you can finally lock the origin to Cloudflare-only:
- Website: enable the mTLS gate already shipped in the web repo
  (`WEB_ORIGIN_MTLS`, see `zerogex-web/deploy/CLOUDFLARE_MTLS.md`).
- API: add the same Authenticated Origin Pulls (`ssl_verify_client`) to the
  `api.zerogex.io` server block, or — cleaner — restrict the origin firewall
  (ufw) `:443` to Cloudflare's IP ranges now that *nothing* needs direct origin
  access on 443.
