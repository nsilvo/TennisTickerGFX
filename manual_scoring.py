"""
File: manual_scoring.py
Description: Point-by-point tennis/padel scoring engine for commentator manual scoring.
             Pure functions over a JSON-serialisable state dict; server.py persists the
             state and overlays it onto the feed's match row.
Author: Nathan Silveston
Contact: nathan@nkpa.co.uk | +44 7515 018048
Copyright (c) 2025 Nathan Silveston. All rights reserved.
"""
import copy

HISTORY_LIMIT = 400          # undo depth
POINT_NAMES = ("00", "15", "30", "40")

DEFAULT_RULES = {
    "best_of": 3,            # sets in the match (3 or 5)
    "games": 6,              # games to win a set
    "tiebreak_at": 6,        # tiebreak played at 6-6
    "tiebreak_points": 7,
    "final_set": "full",     # "full" set, or "mtb" = match tiebreak instead of a final set
    "mtb_points": 10,
    "golden_point": False,   # deciding point at deuce (no-ad / padel golden point)
}


def default_rules(padel=False):
    rules = dict(DEFAULT_RULES)
    if padel:
        rules["final_set"] = "mtb"
    return rules


def new_state(rules=None, server=1):
    return {
        "rules": dict(rules or DEFAULT_RULES),
        "sets": [],              # completed sets: [p1_games, p2_games, tiebreak_loser_points_or_None]
        "games": [0, 0],         # current set
        "points": [0, 0],        # current game (or tiebreak) points as counts
        "server": server,        # 1 or 2
        "tb_first_server": 0,    # who served the first point of the current tiebreak
        "winner": 0,
        "history": [],
    }


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


def in_match_tiebreak(state):
    """Deciding set replaced by a match tiebreak (e.g. 6-4 4-6 10-7)."""
    r = state["rules"]
    won = sets_won(state)
    return (not state["winner"] and r["final_set"] == "mtb"
            and won[0] == won[1] == sets_to_win(state) - 1)


def in_tiebreak(state):
    at = state["rules"]["tiebreak_at"]
    return in_match_tiebreak(state) or state["games"] == [at, at]


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
    snap = {k: copy.deepcopy(v) for k, v in state.items() if k != "history"}
    state["history"].append(snap)
    del state["history"][:-HISTORY_LIMIT]


def _complete_set(state, p1, p2, tb_loser_points):
    state["sets"].append([p1, p2, tb_loser_points])
    state["games"] = [0, 0]
    state["points"] = [0, 0]
    won = sets_won(state)
    if max(won) >= sets_to_win(state):
        state["winner"] = 1 if won[0] > won[1] else 2


def _tiebreak_server(state):
    """Server rotates after the first tiebreak point, then every two points."""
    played = sum(state["points"])
    first = state["tb_first_server"] or state["server"]
    return first if ((played + 1) // 2) % 2 == 0 else 3 - first


def point(state, side):
    """Award a point to side 1 or 2."""
    if state["winner"] or side not in (1, 2):
        return state
    _snapshot(state)
    me, opp = side - 1, 2 - side
    r = state["rules"]

    if in_tiebreak(state):
        mtb = in_match_tiebreak(state)
        if not state["tb_first_server"]:
            state["tb_first_server"] = state["server"]
        state["points"][me] += 1
        target = r["mtb_points"] if mtb else r["tiebreak_points"]
        pts = state["points"]
        if pts[me] >= target and pts[me] - pts[opp] >= 2:
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
        else:
            state["server"] = _tiebreak_server(state)
        return state

    state["points"][me] += 1
    pts = state["points"]
    golden = r["golden_point"] and pts[me] == 4 and pts[opp] == 3
    if (pts[me] >= 4 and pts[me] - pts[opp] >= 2) or golden:
        _win_game(state, me)
    elif r["golden_point"] is False and pts[me] >= 4 and pts[opp] >= 4 and pts[me] == pts[opp]:
        state["points"] = [3, 3]   # back to deuce, keep counts small
    return state


def _win_game(state, me):
    r = state["rules"]
    opp = 1 - me
    state["games"][me] += 1
    state["points"] = [0, 0]
    state["server"] = 3 - state["server"]
    g = state["games"]
    if g[me] >= r["games"] and g[me] - g[opp] >= 2:
        _complete_set(state, g[0], g[1], None)


def undo(state):
    if state["history"]:
        prev = state["history"].pop()
        history = state["history"]
        state.clear()
        state.update(prev)
        state["history"] = history
    return state


def set_server(state, side):
    if side in (1, 2) and side != state["server"]:
        _snapshot(state)
        state["server"] = side
        if in_tiebreak(state) and not sum(state["points"]):
            state["tb_first_server"] = 0
    return state


def adjust_games(state, side, delta):
    """Correction: nudge the current set's games for one side (no set completion)."""
    if side not in (1, 2) or state["winner"]:
        return state
    _snapshot(state)
    g = state["games"]
    g[side - 1] = max(0, g[side - 1] + delta)
    state["points"] = [0, 0]
    return state


def set_rules(state, rules):
    _snapshot(state)
    merged = dict(state["rules"])
    for key, default in DEFAULT_RULES.items():
        if key not in rules:
            continue
        value = rules[key]
        if isinstance(default, bool):
            merged[key] = bool(value)
        elif isinstance(default, int):
            try:
                merged[key] = max(1, int(value))
            except (TypeError, ValueError):
                pass
        elif key == "final_set" and value in ("full", "mtb"):
            merged[key] = value
    if merged["best_of"] not in (1, 3, 5):
        merged["best_of"] = 3
    state["rules"] = merged
    return state


# --------------------------------------------------------------------
# Feed <-> state
# --------------------------------------------------------------------

def _set_won(a, b, rules):
    return (a >= rules["games"] and a - b >= 2) or (a == rules["tiebreak_at"] + 1 and b == rules["tiebreak_at"]) \
        or (a >= rules["mtb_points"] and a - b >= 2 and a > rules["games"] + 1)


def state_from_match(match, rules, max_sets=11):
    """Seed a state from the feed's current score so the commentator carries on from it."""
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
        return state

    raw = (str(match.get("game1") or "").strip().upper(), str(match.get("game2") or "").strip().upper())
    if in_tiebreak(state):
        state["points"] = [int(x) if x.isdigit() else 0 for x in raw]
    else:
        names = {"0": 0, "00": 0, "15": 1, "30": 2, "40": 3, "A": 4, "AD": 4}
        p = [names.get(x, 0) for x in raw]
        if 4 in p:
            p = [4, 3] if p[0] == 4 else [3, 4]
        state["points"] = p
    return state


def apply_to_match(match, state, max_sets=11):
    """Return a copy of the feed match row with the manual score written over it."""
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
    if state["winner"]:
        m["matchstatus"] = "(completed)"
        m["winner"] = str(state["winner"])
        m["winner_name"] = match.get("player1") if state["winner"] == 1 else match.get("player2")
    else:
        m["matchstatus"] = "(in progress)"
        m["winner"] = ""
        m["winner_name"] = ""
    m["manual"] = True
    return m


def summary(state):
    """Compact view of the state for the scoring UI (no undo history)."""
    g1, g2 = point_labels(state)
    return {
        "rules": state["rules"],
        "sets": state["sets"],
        "games": state["games"],
        "points": [g1, g2],
        "server": state["server"],
        "winner": state["winner"],
        "in_tiebreak": in_tiebreak(state),
        "in_match_tiebreak": in_match_tiebreak(state),
        "can_undo": bool(state["history"]),
    }
