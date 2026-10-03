"""
File: manual_scoring.py
Description: Point-by-point tennis/padel scoring engine for commentator and courtside scoring.
             Pure functions over a JSON-serialisable state dict; server.py persists the
             state and overlays it onto the feed's match row.
Author: Nathan Silveston
Contact: nathan@nkpa.co.uk | +44 7515 018048
Copyright (c) 2025 Nathan Silveston. All rights reserved.
"""
import copy
import time

HISTORY_LIMIT = 400          # undo depth
POINT_NAMES = ("00", "15", "30", "40")

DEFAULT_RULES = {
    "best_of": 3,                 # sets in the match (1, 3 or 5)
    "games": 6,                   # games to win a set
    "tiebreak_at": 6,             # tiebreak played at 6-6
    "tiebreak_points": 7,
    "tiebreak_sudden_death": False,  # Fast4: first to the target wins the tiebreak, no 2-point margin
    # Deciding set: "full" (same as other sets), "advantage" (no tiebreak, win by two games),
    # "tb10" (tiebreak to mtb_points at tiebreak_at all), "mtb" (match tiebreak instead of a set)
    "final_set": "full",
    "mtb_points": 10,
    # Deuce: "advantage", "golden" (deciding point at first deuce, no-ad) or
    # "star" (padel star point: deciding point once deuce is reached star_deuces times)
    "deuce_mode": "advantage",
    "star_deuces": 3,
    "golden_point": False,        # kept for older saved states; same as deuce_mode "golden"
}

# Ready-made formats for the scoring page
PRESETS = {
    "tennis_bo3": {"label": "Tennis · best of 3 sets", "rules": {}},
    "tennis_bo3_mtb": {"label": "Tennis · best of 3, match tiebreak decider", "rules": {"final_set": "mtb"}},
    "tennis_bo3_noad": {"label": "Tennis · best of 3, no-ad, match tiebreak", "rules": {"final_set": "mtb", "deuce_mode": "golden"}},
    "tennis_bo5": {"label": "Tennis · best of 5 sets", "rules": {"best_of": 5}},
    "tennis_bo5_tb10": {"label": "Tennis · best of 5, 10-point tiebreak at 6-6 in the 5th", "rules": {"best_of": 5, "final_set": "tb10"}},
    "tennis_bo3_adv": {"label": "Tennis · best of 3, advantage final set", "rules": {"final_set": "advantage"}},
    "short_sets": {"label": "Short sets · first to 4, tiebreak at 4-4", "rules": {"games": 4, "tiebreak_at": 4, "final_set": "mtb"}},
    "fast4": {"label": "Fast4 · first to 4, no-ad, tiebreak at 3-3 to 5", "rules": {"games": 4, "tiebreak_at": 3, "tiebreak_points": 5, "tiebreak_sudden_death": True, "deuce_mode": "golden"}},
    "one_set": {"label": "One set · tiebreak at 6-6", "rules": {"best_of": 1}},
    "padel_bo3": {"label": "Padel · best of 3 sets, advantage", "rules": {}},
    "padel_golden": {"label": "Padel · best of 3, golden point", "rules": {"deuce_mode": "golden"}},
    "padel_star": {"label": "Padel · best of 3, star point", "rules": {"deuce_mode": "star"}},
    "padel_mtb": {"label": "Padel · 2 sets + match tiebreak", "rules": {"final_set": "mtb"}},
    "padel_golden_mtb": {"label": "Padel · golden point, match tiebreak decider", "rules": {"deuce_mode": "golden", "final_set": "mtb"}},
}

POINT_KINDS = ("normal", "ace", "double_fault", "winner", "forced_error", "unforced_error", "penalty")
STATUSES = ("warmup", "live", "suspended", "finished")
END_REASONS = ("", "retired", "walkover", "default")


def default_rules(padel=False):
    rules = dict(DEFAULT_RULES)
    if padel:
        rules["final_set"] = "mtb"
    return rules


def new_state(rules=None, server=1):
    return ensure({
        "rules": dict(rules or DEFAULT_RULES),
        "sets": [],              # completed sets: [p1_games, p2_games, tiebreak_loser_points_or_None]
        "games": [0, 0],         # current set
        "points": [0, 0],        # current game (or tiebreak) points as counts
        "server": server,        # 1 or 2
        "tb_first_server": 0,    # who served the first point of the current tiebreak
        "winner": 0,
        "history": [],
    })


def ensure(state):
    """Fill in keys added since a state was saved (older states keep working)."""
    rules = state.setdefault("rules", dict(DEFAULT_RULES))
    for key, value in DEFAULT_RULES.items():
        rules.setdefault(key, value)
    if rules.get("golden_point") and rules.get("deuce_mode") == "advantage":
        rules["deuce_mode"] = "golden"
    state.setdefault("serve", 1)           # 1st or 2nd serve of the current point
    if "deuces" not in state:              # times deuce reached in the current game
        state["deuces"] = 1 if state.get("points") == [3, 3] else 0
    state.setdefault("status", "live")
    state.setdefault("end_reason", "")
    state.setdefault("confirmed", False)   # result marked complete by the scorer
    state.setdefault("started_at", 0)
    state.setdefault("ended_at", 0)
    state.setdefault("log", [])            # one entry per point (stats); not part of undo snapshots
    state.setdefault("history", [])
    return state


# --------------------------------------------------------------------
# Derived state
# --------------------------------------------------------------------

def sets_won(state):
    won = [0, 0]
    for p1, p2, _tb in state["sets"]:
        won[0 if p1 > p2 else 1] += 1
    return won


def sets_to_win(state):
    return state["rules"]["best_of"] // 2 + 1


def in_final_set(state):
    won = sets_won(state)
    return won[0] == won[1] == sets_to_win(state) - 1


def in_match_tiebreak(state):
    """Deciding set replaced by a match tiebreak (e.g. 6-4 4-6 10-7)."""
    return not state["winner"] and state["rules"]["final_set"] == "mtb" and in_final_set(state)


def in_tiebreak(state):
    r = state["rules"]
    if in_match_tiebreak(state):
        return True
    if in_final_set(state) and r["final_set"] == "advantage":
        return False
    at = r["tiebreak_at"]
    return state["games"] == [at, at]


def _tiebreak_target(state):
    r = state["rules"]
    if in_match_tiebreak(state) or (in_final_set(state) and r["final_set"] == "tb10"):
        return r["mtb_points"]
    return r["tiebreak_points"]


def deciding_point(state):
    """'golden' / 'star' when the next point decides the game at deuce, else ''."""
    if in_tiebreak(state) or state["points"] != [3, 3]:
        return ""
    mode = state["rules"]["deuce_mode"]
    if mode == "golden" and state["deuces"] >= 1:
        return "golden"
    if mode == "star" and state["deuces"] >= state["rules"]["star_deuces"]:
        return "star"
    return ""


def point_labels(state):
    """Display strings for the current game: '00'/'15'/'30'/'40'/'AD', or tiebreak counts."""
    p1, p2 = state["points"]
    if state["winner"]:
        return "", ""
    if in_tiebreak(state):
        return str(p1), str(p2)
    if p1 >= 3 and p2 >= 3:
        if p1 == p2:
            return "40", "40"
        return ("AD", "40") if p1 > p2 else ("40", "AD")
    return POINT_NAMES[min(p1, 3)], POINT_NAMES[min(p2, 3)]


# --------------------------------------------------------------------
# Actions (each pushes an undo snapshot first)
# --------------------------------------------------------------------

def _snapshot(state):
    snap = {k: copy.deepcopy(v) for k, v in state.items() if k not in ("history", "log")}
    snap["log_len"] = len(state["log"])
    state["history"].append(snap)
    del state["history"][:-HISTORY_LIMIT]


def _complete_set(state, p1, p2, tb_loser_points):
    state["sets"].append([p1, p2, tb_loser_points])
    state["games"] = [0, 0]
    state["points"] = [0, 0]
    won = sets_won(state)
    if max(won) >= sets_to_win(state):
        state["winner"] = 1 if won[0] > won[1] else 2
        state["status"] = "finished"
        state["ended_at"] = time.time()


def _tiebreak_server(state):
    """Server rotates after the first tiebreak point, then every two points."""
    played = sum(state["points"])
    first = state["tb_first_server"] or state["server"]
    return first if ((played + 1) // 2) % 2 == 0 else 3 - first


def _game_point_for_receiver(state):
    """True when the receiver wins the game (a break) by winning the next point."""
    if in_tiebreak(state):
        return False
    receiver = 3 - state["server"]
    me, opp = receiver - 1, 2 - receiver
    p = state["points"]
    if deciding_point(state):
        return True
    return p[me] >= 3 and p[me] - p[opp] >= 1


def _award(state, side, kind, serve_no):
    """Core scoring for one point won by `side`; returns the side that won a game (0 if none)."""
    me, opp = side - 1, 2 - side
    r = state["rules"]

    if in_tiebreak(state):
        mtb = in_match_tiebreak(state)
        if not state["tb_first_server"]:
            state["tb_first_server"] = state["server"]
        state["points"][me] += 1
        target = _tiebreak_target(state)
        pts = state["points"]
        sudden = r["tiebreak_sudden_death"] and not mtb
        if pts[me] >= target and (sudden or pts[me] - pts[opp] >= 2):
            # The player who received first in the tiebreak serves the next set
            next_server = 3 - state["tb_first_server"]
            state["tb_first_server"] = 0
            if mtb:
                _complete_set(state, pts[0], pts[1], None)
            else:
                games = list(state["games"])
                games[me] += 1
                _complete_set(state, games[0], games[1], min(pts))
            state["server"] = next_server
            return side
        state["server"] = _tiebreak_server(state)
        return 0

    deciding = bool(deciding_point(state))
    state["points"][me] += 1
    pts = state["points"]
    if deciding or (pts[me] >= 4 and pts[me] - pts[opp] >= 2):
        _win_game(state, me)
        return side
    if pts == [3, 3] or (pts[0] >= 4 and pts[0] == pts[1]):
        state["points"] = [3, 3]   # (back to) deuce; keep counts small
        state["deuces"] += 1
    return 0


def _win_game(state, me):
    r = state["rules"]
    opp = 1 - me
    state["games"][me] += 1
    state["points"] = [0, 0]
    state["deuces"] = 0
    state["server"] = 3 - state["server"]
    g = state["games"]
    if g[me] >= r["games"] and g[me] - g[opp] >= 2:
        _complete_set(state, g[0], g[1], None)


def _start_if_needed(state):
    if state["status"] in ("warmup", "suspended"):
        state["status"] = "live"
    if not state["started_at"]:
        state["started_at"] = time.time()


def point(state, side, kind="normal"):
    """Award a point to side 1 or 2. kind: normal, ace, winner, forced_error, unforced_error, penalty, double_fault."""
    ensure(state)
    if state["winner"] or side not in (1, 2):
        return state
    kind = kind if kind in POINT_KINDS else "normal"
    _snapshot(state)
    _start_if_needed(state)
    entry = {
        "w": side, "srv": state["server"], "sn": state["serve"], "t": kind,
        "bp": _game_point_for_receiver(state), "tb": in_tiebreak(state),
        "set": len(state["sets"]) + 1, "ts": int(time.time()),
    }
    game_won_by = _award(state, side, kind, state["serve"])
    if game_won_by and not entry["tb"]:
        entry["gw"] = game_won_by
    state["serve"] = 1
    state["log"].append(entry)
    return state


def ace(state):
    return point(state, state["server"], "ace")


def fault(state):
    """Service fault: 1st serve -> 2nd serve; 2nd serve -> double fault (point to the receiver)."""
    ensure(state)
    if state["winner"]:
        return state
    if state["serve"] == 1:
        _snapshot(state)
        _start_if_needed(state)
        state["serve"] = 2
        return state
    return point(state, 3 - state["server"], "double_fault")


def award_game(state, side):
    """
    Game-by-game scoring (no point detail): award the current game to `side`.
    At the tiebreak score this wins the tiebreak and the set; in a match tiebreak
    it adds a tiebreak point (the match tiebreak is scored in points).
    Not added to the point log, so point statistics stay point-only.
    """
    ensure(state)
    if state["winner"] or side not in (1, 2):
        return state
    _snapshot(state)
    _start_if_needed(state)
    me = side - 1
    if in_match_tiebreak(state):
        _award(state, side, "normal", state["serve"])
    elif in_tiebreak(state):
        games = list(state["games"])
        games[me] += 1
        next_server = 3 - (state["tb_first_server"] or state["server"])
        state["tb_first_server"] = 0
        _complete_set(state, games[0], games[1], None)
        state["server"] = next_server
    else:
        _win_game(state, me)
    state["points"] = [0, 0] if not in_match_tiebreak(state) else state["points"]
    state["deuces"] = 0
    state["serve"] = 1
    return state


def penalty(state, side):
    """Point penalty: point awarded to `side` (code violation)."""
    return point(state, side, "penalty")


def undo(state):
    ensure(state)
    if state["history"]:
        prev = state["history"].pop()
        history, log = state["history"], state["log"]
        log_len = prev.pop("log_len", len(log))
        state.clear()
        state.update(prev)
        state["history"] = history
        state["log"] = log[:log_len]
        ensure(state)
    return state


def set_server(state, side):
    ensure(state)
    if side in (1, 2) and side != state["server"]:
        _snapshot(state)
        state["server"] = side
        state["serve"] = 1
        if in_tiebreak(state) and not sum(state["points"]):
            state["tb_first_server"] = 0
    return state


def adjust_games(state, side, delta):
    """Correction: nudge the current set's games for one side (no set completion)."""
    ensure(state)
    if side not in (1, 2) or state["winner"]:
        return state
    _snapshot(state)
    g = state["games"]
    g[side - 1] = max(0, g[side - 1] + delta)
    state["points"] = [0, 0]
    state["deuces"] = 0
    state["serve"] = 1
    return state


def set_status(state, status):
    """warmup / live / suspended (finished comes from the score or end_match)."""
    ensure(state)
    if status in ("warmup", "live", "suspended") and not state["winner"] and status != state["status"]:
        _snapshot(state)
        state["status"] = status
        if status == "live" and not state["started_at"]:
            state["started_at"] = time.time()
    return state


def end_match(state, winner, reason):
    """Finish early: retirement, walkover or default, with the winning side."""
    ensure(state)
    if winner in (1, 2) and reason in ("retired", "walkover", "default") and not state["winner"]:
        _snapshot(state)
        state["winner"] = winner
        state["end_reason"] = reason
        state["status"] = "finished"
        state["ended_at"] = time.time()
    return state


def awaiting_confirmation(state):
    """Finished but not yet marked complete: the result stays on air for graphics."""
    return bool(ensure(state)["winner"]) and not state["confirmed"]


def confirm_result(state):
    """Scorer marks the finished match complete; the court can move on to its next match."""
    ensure(state)
    if state["winner"] and not state["confirmed"]:
        _snapshot(state)
        state["confirmed"] = True
    return state


def set_rules(state, rules):
    ensure(state)
    _snapshot(state)
    merged = dict(state["rules"])
    for key, default in DEFAULT_RULES.items():
        if key not in rules:
            continue
        value = rules[key]
        if isinstance(default, bool):
            merged[key] = bool(value) and value not in ("0", "false", "off")
        elif isinstance(default, int):
            try:
                merged[key] = max(1, int(value))
            except (TypeError, ValueError):
                pass
        elif key == "final_set" and value in ("full", "advantage", "tb10", "mtb"):
            merged[key] = value
        elif key == "deuce_mode" and value in ("advantage", "golden", "star"):
            merged[key] = value
    if merged["best_of"] not in (1, 3, 5):
        merged["best_of"] = 3
    merged["golden_point"] = merged["deuce_mode"] == "golden"
    state["rules"] = merged
    return state


def rules_for_preset(name, overrides=None):
    rules = dict(DEFAULT_RULES)
    rules.update((PRESETS.get(name) or {}).get("rules", {}))
    rules.update(overrides or {})
    return rules


# --------------------------------------------------------------------
# Stats, flags and summaries
# --------------------------------------------------------------------

def _pct(num, den):
    return round(100 * num / den) if den else None


def stats(state):
    """Per-side match statistics built from the point log."""
    log = ensure(state)["log"]
    out = {}
    for s in (1, 2):
        o = 3 - s
        served = [e for e in log if e["srv"] == s and e["t"] != "penalty"]
        first_in = [e for e in served if e["sn"] == 1 and e["t"] != "double_fault"]
        second = [e for e in served if e["sn"] == 2]
        bp_chances = [e for e in log if e.get("bp") and e["srv"] == o]
        bp_faced = [e for e in log if e.get("bp") and e["srv"] == s]
        service_games = [e for e in log if e.get("gw") and e["srv"] == s]
        out[s] = {
            "points_won": sum(1 for e in log if e["w"] == s),
            "aces": sum(1 for e in served if e["t"] == "ace"),
            "double_faults": sum(1 for e in served if e["t"] == "double_fault"),
            "first_serve_in": len(first_in),
            "serve_points": len(served),
            "first_serve_pct": _pct(len(first_in), len(served)),
            "first_serve_won": sum(1 for e in first_in if e["w"] == s),
            "first_serve_won_pct": _pct(sum(1 for e in first_in if e["w"] == s), len(first_in)),
            "second_serve_points": len(second),
            "second_serve_won": sum(1 for e in second if e["w"] == s),
            "second_serve_won_pct": _pct(sum(1 for e in second if e["w"] == s), len(second)),
            "winners": sum(1 for e in log if e["w"] == s and e["t"] in ("winner", "ace")),
            "forced_errors": sum(1 for e in log if e["w"] == o and e["t"] == "forced_error"),
            "unforced_errors": sum(1 for e in log if e["w"] == o and e["t"] == "unforced_error"),
            "break_points_won": sum(1 for e in bp_chances if e["w"] == s),
            "break_points": len(bp_chances),
            "break_points_saved": sum(1 for e in bp_faced if e["w"] == s),
            "break_points_faced": len(bp_faced),
            "service_games_won": sum(1 for e in service_games if e["gw"] == s),
            "service_games": len(service_games),
            "penalties_received": sum(1 for e in log if e["w"] == o and e["t"] == "penalty"),
        }
    return out


def _simulate(state, side):
    sim = {k: copy.deepcopy(v) for k, v in state.items() if k not in ("history", "log")}
    sim["history"], sim["log"] = [], []
    return point(sim, side)


def point_flags(state):
    """What the next point means for each side: 'match point', 'set point', 'break point', 'game point'."""
    ensure(state)
    flags = {1: "", 2: ""}
    if state["winner"]:
        return flags
    for side in (1, 2):
        sim = _simulate(state, side)
        if sim["winner"] == side:
            flags[side] = "match point"
        elif len(sim["sets"]) > len(state["sets"]):
            flags[side] = "set point"
        elif sim["games"] != state["games"]:
            flags[side] = "break point" if side != state["server"] else "game point"
    return flags


def change_of_ends(state):
    """True when the players change ends before the next point."""
    ensure(state)
    if state["winner"] or not state["log"]:
        return False
    last = state["log"][-1]
    if in_tiebreak(state):
        played = sum(state["points"])
        return played > 0 and played % 6 == 0
    if last.get("gw") or last.get("tb"):
        # Odd total games in the set, or a new set after an odd-game set
        total = sum(state["games"])
        if total == 0 and state["sets"]:
            prev = state["sets"][-1]
            return (prev[0] + prev[1]) % 2 == 1
        return total % 2 == 1
    return False


def summary(state):
    """Compact view of the state for the scoring UIs (no undo history or full log)."""
    ensure(state)
    g1, g2 = point_labels(state)
    flags = point_flags(state)
    now = state["ended_at"] or time.time()
    return {
        "rules": state["rules"],
        "preset": state.get("preset", ""),
        "sets": state["sets"],
        "games": state["games"],
        "points": [g1, g2],
        "server": state["server"],
        "serve": state["serve"],
        "winner": state["winner"],
        "status": state["status"],
        "end_reason": state["end_reason"],
        "confirmed": state["confirmed"],
        "awaiting_confirmation": awaiting_confirmation(state),
        "in_tiebreak": in_tiebreak(state),
        "in_match_tiebreak": in_match_tiebreak(state),
        "deciding_point": deciding_point(state),
        "flags": flags,
        "change_ends": change_of_ends(state),
        "duration_sec": int(now - state["started_at"]) if state["started_at"] else 0,
        "stats": stats(state),
        "recent": state["log"][-12:],
        "can_undo": bool(state["history"]),
    }


# --------------------------------------------------------------------
# Feed <-> state
# --------------------------------------------------------------------

def _set_won(a, b, rules):
    return (a >= rules["games"] and a - b >= 2) or (a == rules["tiebreak_at"] + 1 and b == rules["tiebreak_at"]) \
        or (a >= rules["mtb_points"] and a - b >= 2 and a > rules["games"] + 1)


def state_from_match(match, rules, max_sets=11):
    """Seed a state from the feed's current score so the scorer carries on from it."""
    state = new_state(rules, server=2 if str(match.get("player2serve") or "") == "2" else 1)
    played = int(match.get("sets_played_count") or 0)
    rows = []
    for i in range(1, min(played, max_sets) + 1):
        try:
            a, b = int(match.get(f"set{i}_p1") or 0), int(match.get(f"set{i}_p2") or 0)
        except (TypeError, ValueError):
            continue
        tb = str(match.get(f"set{i}_tb") or "").strip()
        rows.append((a, b, int(tb) if tb.isdigit() else None))

    for a, b, tb in rows:
        if _set_won(a, b, rules) or _set_won(b, a, rules):
            state["sets"].append([a, b, tb])
        else:
            state["games"] = [a, b]
            break

    won = sets_won(state)
    if max(won) >= sets_to_win(state):
        state["winner"] = 1 if won[0] > won[1] else 2
        state["status"] = "finished"
        return state

    raw = (str(match.get("game1") or "").strip().upper(), str(match.get("game2") or "").strip().upper())
    if in_tiebreak(state):
        state["points"] = [int(x) if x.isdigit() else 0 for x in raw]
    else:
        names = {"0": 0, "00": 0, "15": 1, "30": 2, "40": 3, "A": 4, "AD": 4}
        p = [names.get(x, 0) for x in raw]
        if 4 in p:
            p = [4, 3] if p[0] == 4 else [3, 4]
        if p == [3, 3]:
            state["deuces"] = 1
        state["points"] = p
    if rows or any(state["points"]):
        state["started_at"] = time.time()
    return state


STATUS_TO_FEED = {"warmup": "WARMUP", "live": "(in progress)", "suspended": "SUSPENDED", "finished": "(completed)"}


def apply_to_match(match, state, max_sets=11):
    """Return a copy of the feed match row with the manual score written over it."""
    ensure(state)
    m = dict(match)
    for i in range(1, max_sets + 1):
        for k in (f"set{i}_p1", f"set{i}_p2", f"set{i}_tb"):
            m.pop(k, None)
    rows = [(a, b, "" if tb is None else str(tb)) for a, b, tb in state["sets"]]
    if not state["winner"]:
        # In a match tiebreak the set shows 0-0 and the tiebreak points sit in game1/game2
        rows.append((state["games"][0], state["games"][1], ""))
    for i, (a, b, tb) in enumerate(rows[:max_sets], start=1):
        m[f"set{i}_p1"], m[f"set{i}_p2"], m[f"set{i}_tb"] = a, b, tb
    m["sets_played_count"] = len(rows)

    g1, g2 = point_labels(state)
    m["game1"], m["game2"] = g1, g2
    m["player2serve"] = state["server"]
    m["is_plan"] = 0
    m["matchstatus"] = STATUS_TO_FEED.get(state["status"], "(in progress)")
    if state["winner"]:
        m["matchstatus"] = "(completed)"
        m["winner"] = str(state["winner"])
        m["winner_name"] = match.get("player1") if state["winner"] == 1 else match.get("player2")
    else:
        m["winner"] = ""
        m["winner_name"] = ""
    m["manual"] = True
    # Extra detail for graphics (vMix columns)
    flags = point_flags(state)
    m["serve_number"] = "" if state["winner"] else str(state["serve"])
    m["point_flag"] = (deciding_point(state) + " point").upper() if deciding_point(state) else \
        (flags[1] or flags[2]).upper()
    m["result_note"] = {"retired": "Ret.", "walkover": "W/O", "default": "Def."}.get(state["end_reason"], "")
    # Finished but not yet marked complete: keeps the court's on-air slot (winner graphics)
    m["awaiting_confirmation"] = awaiting_confirmation(state)
    m["manual_stats"] = stats(state)
    return m
