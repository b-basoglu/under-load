# Part 20 lab: graceful degradation

`degradation.py` — real threads, wall-clock time: a ticket page at 40 loads/s built from a core (ticket + comments) and
three optional widgets; the search cluster behind "related tickets" hangs from 5–15 s. Compares all-or-nothing (every
widget required, 2 s timeouts), degrading (150 ms shared deadline + placeholders) and degrading with a kill switch.
Results in `degradation_results.json`. Last run (after the 2026-10-25 audit): 3/3 passed. Pure Python; about a minute.

```bash
python3 degradation.py
```
