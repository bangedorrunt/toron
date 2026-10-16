#!/usr/bin/env python3
"""mkenv.py OUT KIND FROM SEQ TO CTX [TASK|ROLE GEN]: write one envelope JSON file."""
import json, sys
out, kind, frm, seq, to, ctx = sys.argv[1:7]
extra = sys.argv[7:]
env = {"v": 1, "msg": {"producer": frm, "epoch": 0, "seq": int(seq)}, "from": frm,
       "to": {"npub": to}, "context": ctx, "gen": 1}
if kind == "role":
    role, gen = extra
    env.update(class_="control", contract={"name": "role", "major": 1}, gen=0,
               body={"npub": to, "role": role, "gen": int(gen)})
elif kind == "task":
    env.update(class_="task", contract={"name": "task", "major": 1}, task=extra[0],
               body={"title": "t", "brief": "do the thing", "scope_paths": ["src/**"]})
elif kind == "notify":
    env.update(class_="notify", contract={"name": "notify", "major": 1}, body={"text": "hello"})
elif kind == "verdict":
    env.update(class_="control", contract={"name": "verdict", "major": 1}, task=extra[0],
               body={"pass": True, "evidence_refs": ["cargo test"]})
else:
    sys.exit("kind?")
env["class"] = env.pop("class_")
json.dump(env, open(out, "w"))
