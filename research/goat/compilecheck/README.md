# Compiling the strategy without NinjaTrader

`ZeroGexGoat.cs` targets NinjaTrader 8, which does not run here, so it would
otherwise ship unverified — and it ships to a customer who compiles it himself.

`NtStub.cs` is a minimal stand-in for the NT8 API surface the strategy touches,
with the same signatures. Compiling the real file against it catches syntax
errors, typos, wrong argument counts and unassigned locals before they reach
his NinjaScript Editor:

    mcs -target:library -out:/dev/null -warn:4 \
        research/goat/compilecheck/NtStub.cs research/goat/ZeroGexGoat.cs

Clean exit means the file is syntactically valid and internally consistent.

It does NOT mean the NT8 API matches these stubs. Order-handling semantics,
indicator behavior and anything that only shows up at runtime are still only
verifiable in NinjaTrader. Treat a clean run as "this will not fail on paste",
not as "this is correct".

Install the compiler with `apt-get install -y mono-mcs`.
