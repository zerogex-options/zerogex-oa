# Non-Converter Outreach — Templates, Targeting, and Reply Triage

_Last updated: 2026-06-16_

This is the operational playbook for Move #1 in the growth roadmap: while
the user base is still 277 signups and the population fits in a spreadsheet,
**personally email and DM every non-converter and trial drop-off**. The
qualitative answers you get back tell you the single highest-leverage thing
to fix, and they're impossible to retrieve at 2,000+ users.

Cohorts come from PostHog (see `posthog_funnel_setup.md` §4). Send-cadence
target: **30–40 personalized emails per day**, finish the backlog inside a
week so each conversation is fresh in your head when the next one lands.

---

## 0. Ground rules

1. **One-to-one, not blast.** These emails come from `you@zerogex.io` with
   a real reply-to. The whole value is that someone replies; a "do not
   reply" automation tells the recipient you're not actually interested.
2. **Personalize one line per send.** The PostHog Person view tells you
   what they did and didn't do. Reference _their_ behavior in the first
   sentence — that's the entire reason they read past it.
3. **Ask one question.** Two questions reduce reply rate to a fraction.
4. **No discount in the first send.** A discount is the obvious offer and
   it'll get a "sure" with no insight. The whole point is to learn the
   _reason_ they didn't convert. A discount is leverage for the second
   send, only to the people who replied with something fixable.
5. **No founder-mode word-vomit.** Short. Plain text. No images, no HTML
   chrome, no logos. Looks like a human wrote it because a human did.
6. **Track replies in a single spreadsheet** with three columns: cohort,
   verbatim reason, what we'd need to change. Two-thirds of "the leak" is
   visible from twelve replies in that sheet.

---

## 1. Cohort A — Signed up, never started a trial

These users signed up, may have looked around, then never hit Subscribe.
The most common true reasons (in our category, based on the SpotGamma /
Unusual Whales playbooks): unclear what's paywalled vs free, unsure the
tool covers their setup (0DTE? SPX? earnings plays?), nervous about a
credit card on a brand-new site.

### Email — Cohort A (sub: "1 quick question about ZeroGEX")

> Subject: 1 quick question about ZeroGEX
>
> Hi {first_name},
>
> You signed up for ZeroGEX on {signup_date} but didn't end up starting
> the trial — totally fine, no pitch coming.
>
> I'm trying to figure out where we lost you so we can fix it for the
> next person. If you've got 30 seconds: what was the thing that made
> you decide not to start the trial? Anything from "pricing wasn't clear"
> to "I forgot" to "the dashboard didn't show what I expected" is useful.
>
> Just hit reply — goes straight to me.
>
> — {sender_first_name}
> ZeroGEX

**Personalization slot** (insert between paragraph 1 and 2 when PostHog
shows a meaningful behavior):

- _If `first_value` fired:_ "I see you got as far as opening the gamma
  exposure dashboard — curious if what you saw matched or missed what you
  were hoping for."
- _If they hit /pricing and bounced:_ "Looks like you got to the pricing
  page — was there a tier question or did the price not feel right for
  what's there?"
- _If they never returned after signup:_ "Looks like life got in the
  way after you signed up — was it timing, or something on the site?"

### DM — Cohort A (X / Twitter)

> Hey — noticed you signed up at zerogex.io a couple weeks back but
> didn't try the trial. Mind sharing what tipped it over from "yes" to
> "later"? Trying to fix the leak while we're still small enough to ask.

---

## 2. Cohort B — Started a trial, didn't convert

These are the highest-value replies. The product made enough of a first
impression that they put in a card, then 7 days later said no. The reason
is almost always one of: (a) didn't get a chance to use it enough to form
an opinion, (b) used it and didn't see the edge they expected, (c) used
it and liked it but the price didn't pencil out for their account size.
All three are fixable but each has a different fix.

### Email — Cohort B (sub: "Your ZeroGEX trial — what went wrong?")

> Subject: Your ZeroGEX trial — what went wrong?
>
> Hi {first_name},
>
> Saw your trial ended on {trial_end_date} without converting. I'm
> emailing every trial-drop-off personally because we're still small
> enough that I can — and what you say to me literally shapes what we
> build next.
>
> The honest version of my question: was it that you didn't get enough
> time to use it, that you used it and it didn't earn its keep, or that
> the price didn't make sense for your account? Or something else
> entirely? Whatever it is, I'd rather hear it than guess.
>
> Hit reply with one line. That's all I need.
>
> — {sender_first_name}
> ZeroGEX

**Personalization slot:**
- _If `first_value` never fired:_ "Looking at the logs, it doesn't look
  like the dashboard ever fully loaded for you — if it was a tech issue I
  want to know about it specifically, that's on us to fix."
- _If they used the GEX dashboard but not signals:_ "Saw you spent most
  of the trial on the gamma dashboard and didn't dig into the advanced
  signals — totally fine, but if there was something blocking the rest
  of the tool from being useful, that's the part I want to fix."
- _If they were active right up to the last day:_ "It's weird because
  you were using the dashboard right up until the end — usually that
  pattern means it was close. What would have tipped it?"

### DM — Cohort B (X / Twitter)

> Hey — your ZeroGEX trial ended without you converting. Two options:
> wasn't enough trial time, or wasn't worth the price for what you
> needed? Either's fixable; the answer just tells me which.

### Second send — Cohort B, replied with "price"

Only sent to people who replied to the first email with something
price-related. This is the only send where a discount is on the table,
and only because we've already learned that the leak for this specific
person _is_ the price.

> Subject: Re: Your ZeroGEX trial — what went wrong?
>
> Thanks for replying.
>
> Since you took the trial seriously and the math just didn't pencil
> out, I want to offer you the **founding-member rate** ({founding_rate_text})
> — same offer the first cohort got. It's not a marketing thing; it's the
> price I think actually clears for serious retail options traders, and
> if you'd convert at that number it tells me we should be there for
> everyone next quarter.
>
> If you want it, just reply "yes" and I'll send the checkout link
> direct.
>
> — {sender_first_name}

---

## 3. Cohort C — Founding members on trial, deciding by July 1

~125 users on the founding-member trial. Many will lapse silently on
July 1 if no one talks to them. They're the most valuable cohort by
construction — they cared enough to take the founding offer.

### Email — Cohort C, sent ~7 days before July 1

> Subject: {first_name}, your founding rate is locked in until July 1
>
> Hi {first_name},
>
> Your founding-member trial converts (or doesn't) on July 1, and I
> wanted to write you directly rather than let it lapse silently.
>
> I'm not going to pitch you the product — you've been using it, so you
> already know whether it earns its keep. What I'd actually like to
> know: what would make this an **obvious** keep for you? Is there
> something the dashboards almost-but-not-quite cover for your setup?
> A signal you wish existed? A view that's close but not how you'd
> consume it?
>
> Reply with whatever's top of mind. If you want to keep the founding
> rate, just say so and you're set — the checkout's already wired to
> your account.
>
> — {sender_first_name}
> ZeroGEX

**Personalization slots:**
- _If active in the last 3 days:_ "I noticed you were on the dashboard
  again this week — that's the kind of usage pattern that usually means
  it's working, so I'm curious what would push it from 'working' to
  'must-have'."
- _If inactive >7 days:_ "Looks like you haven't logged in in a couple
  weeks — I'd rather hear that it didn't click than have you quietly
  lapse on July 1. What missed?"
- _If they hit the trade-signals page heavily:_ "You spent a lot of the
  trial inside the trade-signals views — if there's something specific
  there you want to see expanded or refined, I want to know."

### Email — Cohort C, sent 24 hours before lapse (only to non-repliers)

> Subject: Founding rate ends tomorrow
>
> Hi {first_name},
>
> Tomorrow's the cutoff for the founding rate. After that the price
> goes to the regular tier for everyone, including the next cohort.
>
> If you want to keep it, reply "yes" or hit {checkout_link}. If not,
> no hard feelings — but I'd really like to know in one line what
> didn't click. That's worth more to me than the conversion.
>
> — {sender_first_name}

---

## 4. The reply-handling rubric

Replies come in three flavors. Sort each into the right bucket the moment
it lands; don't let them pile up unsorted.

### "Fixable on our side" (the gold)
Examples: "I didn't realize the advanced signals were behind the trial",
"The dashboard never loaded on mobile Safari", "I wanted CSV export and
couldn't find it", "0DTE coverage on QQQ wasn't there".

**Action:** add to a single `growth/feedback_intake.md` doc (one line per
report). At 5 reports of the same thing, **build it this week**. At
10 reports, **announce a public changelog entry** so the cohort sees the
feedback loop is real — that's what turns "I gave feedback once" into
"I'm telling my Discord about this tool."

### "Not for me" (move on)
Examples: "I'm using TradingView and don't trade options seriously",
"I'm a day trader of futures, not options". **Action:** reply once,
thank them, ask if they know one person who'd be the right fit. Don't
re-engage. The cohort isn't the target.

### "Price" (negotiate, but only after the data)
Wait for at least 5 of these before deciding the price is wrong. Five is
a small number but the cohort is small. If 5 of ~30 replies cite price
and the rest are content, the founder-discount path in §2 is the right
local lever. If 15 of 30 cite price, the pricing _is_ wrong — drop the
list price and re-cohort the next 30 days against the new number.

---

## 5. Personalization variables — where they come from

| Token                  | Source                                                                 |
|------------------------|------------------------------------------------------------------------|
| `{first_name}`         | `users.first_name` in the website DB; PostHog Person `name` falls back |
| `{signup_date}`        | `users.created_at`                                                     |
| `{trial_end_date}`     | Stripe subscription `trial_end`                                        |
| `{founding_rate_text}` | Hard-coded copy block — keep in `growth/founding_rate_copy.md`         |
| `{checkout_link}`      | Direct Stripe checkout URL for the founding price                      |
| `{sender_first_name}`  | Sender (whoever's running the outreach)                                |

For the small-cohort outreach we're doing this round, **don't build the
templated send infrastructure** — open Resend (or whatever's wired) and
copy/paste, one at a time, with the personalization tweak in place. The
goal here is replies, not throughput.

---

## 6. Time-boxing

If this drags out, the cohort goes cold and the qualitative gold dies.
Suggested cadence:

- **Day 0 (today):** export the three cohorts from PostHog. ~277 - 15 paid
  ≈ 262 to reach.
- **Day 1–2:** Cohort C (founding members) first — they have the July 1
  deadline. ~125 emails.
- **Day 3–4:** Cohort B (trial drop-offs). Smaller cohort, highest signal.
- **Day 5–7:** Cohort A (never-trialed signups). Lowest signal per email,
  largest cohort, batch through last.
- **Day 7–14:** triage replies, ship the top one or two fixes, send the
  conversion-rate update to the cohort that gave the feedback. Loop closed.

After this round, **don't** plan to do this at every size. This is a
one-shot at the cold-start sample where the signal-to-effort ratio is
absurdly favorable. By 2,000 users the same effort buys you maybe 1/8 the
insight per email; by then the funnel data itself does the heavy lifting.
