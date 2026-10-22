"""Standalone regression checks for the OMS gateway's kill switch.

Run from the repo root:  python3 test_kill_switch.py

Covers the rule that matters: the kill switch blocks NEW RISK (BUY) but must NEVER
block an exit (SELL). A panic button that also freezes stop-outs is how a losing
position stays open all day.
"""
import ast
import os
import sys

SRC = "server.py"
FAILS = []


def check(name, cond, extra=""):
    if cond:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}" + (f"  -> {extra}" if extra else ""))
        FAILS.append(name)


def load_function(name, namespace=None):
    """Pull a single module-level function out of server.py and exec it, so the rule
    can be exercised without importing the whole gateway (alpaca, postgres, threads)."""
    tree = ast.parse(open(SRC).read())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = namespace or {}
    exec(compile(mod, "<mini>", "exec"), ns)
    return ns[name]


def test_rule():
    print("\n[rule] the kill switch blocks BUY and never blocks SELL")
    ns = {"GLOBAL_TRADING_HALTED": False}
    blocks = load_function("kill_switch_blocks", ns)
    check("not halted -> nothing blocked", blocks("BUY") is False)
    ns["GLOBAL_TRADING_HALTED"] = True
    check("halted -> BUY blocked", blocks("BUY") is True)
    check("halted -> SELL allowed", blocks("SELL") is False)
    check("halted -> lowercase sell allowed", blocks("sell") is False)
    check("halted -> partial fill side allowed", blocks("Sell") is False)
    check("halted -> other buy sides blocked", blocks("buy") is True)


def test_static():
    print("\n[static] the gateway's invariants")
    src = open(SRC).read()

    check("kill switch state is in memory", "GLOBAL_TRADING_HALTED = False" in src)
    check("config is persisted to disk", "kill_switch_config.json" in src)
    check("config is loaded at import", "load_kill_switch_config()" in src)
    check("status endpoint exists", "@app.route('/api/kill_switch', methods=['GET'])" in src)
    check("control endpoint exists", "@app.route('/api/kill_switch', methods=['POST'])" in src)
    check("auto breaker can be disabled", '"auto_breaker"' in src)
    check("threshold can be overridden", '"threshold"' in src)
    check("manual latch is supported", '"latch"' in src)
    check("latch auto-releases on recovery", "manual latch released" in src)
    check("breaker threshold is configurable",
          'float(KILL_SWITCH_CONFIG.get("threshold", -0.03))' in src)
    check("breaker respects the auto_breaker switch",
          "elif auto_breaker and daily_dd <= threshold:" in src)

    # the gate must sit in place_order, and only there
    pi = src.index("def place_order()")
    body = src[pi:pi + 3000]
    check("place_order consults the rule", "kill_switch_blocks(side)" in body)
    check("place_order does not block SELL directly",
          'GLOBAL_TRADING_HALTED and side != "SELL"' not in body)

    # flatten and cancel must never be gated
    for route in ("/api/flatten", "/api/cancel"):
        idx = src.index(f"self.path == '{route}'") if f"self.path == '{route}'" in src else src.index(route)
        seg = src[idx:idx + 2500]
        check(f"{route} is not gated by the kill switch",
              "kill_switch_blocks" not in seg and "GLOBAL_TRADING_HALTED" not in seg)


if __name__ == "__main__":
    test_rule()
    test_static()
    print()
    if FAILS:
        print(f"FAILED ({len(FAILS)}): {FAILS}")
        sys.exit(1)
    print("ALL CHECKS PASSED")
