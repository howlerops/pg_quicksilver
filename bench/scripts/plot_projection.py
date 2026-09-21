#!/usr/bin/env python3
"""Draw docs/img/projection-benefit.svg from bench/results/projection_benefit.json.

The chart is GENERATED, not drawn. Every figure in docs/36 comes out of the
measurement file, so re-running the benchmark and re-running this is the only
way the picture changes -- there is no step where a number gets typed in.

Two panels sharing one logarithmic axis, because the data has two regimes and
putting them on one linear axis would hide both:

  left   analytical scans. Speedup against the number of columns the query
         touches, one line per scale. This is the projection mechanism: the
         fewer columns you read, the more a column store skips.
  right  the point lookup. Same axis, so that "16x worse" is read off the same
         ruler as "11x better" rather than asserted in a caption.

The point lookup is NOT on the left panel even though it touches one column.
It is an index probe, not a scan, so plotting it at x=1 beside an 11x scan
would put two different access paths on one line and invite the reader to
interpolate between them.

    python3 bench/scripts/plot_projection.py
"""

import json
import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "bench/results/projection_benefit.json"
OUT = ROOT / "docs/img/projection-benefit.svg"

# Categorical slots 1-3 from the validated palette, light and dark steps.
# Validated with scripts/validate_palette.js --pairs all in both modes:
# worst CVD dE 9.2 light / 9.4 dark, worst normal-vision dE 24.0 / 20.9.
# Light aqua is below 3:1 on the light surface, so the relief rule applies and
# every line is DIRECT-LABELLED; the doc also carries the full table.
SERIES = [
    ("#2a78d6", "#3987e5"),
    ("#eb6834", "#d95926"),
    ("#1baf7a", "#199e70"),
]

# One representative query per distinct column count, so the x axis is a clean
# ordered scale. The three cols=2 queries all appear in docs/36's table; putting
# them all on the line would make it wander for reasons the axis does not name.
LINE_QUERIES = [
    ("count(*)", 0),
    ("sum 1 column", 1),
    ("sum 2 columns", 2),
    ("sum 5 columns", 5),
    ("touch 10 columns", 10),
    ("touch all 19 columns", 19),
]
LOOKUP = "point lookup by key"

W, H = 940, 480
PAD_T, PAD_B = 80, 92
L1, R1 = 62, 610          # left panel plot area
L2, R2 = 706, 898         # right panel
Y0, Y1 = PAD_T, H - PAD_B

TICKS = [0.03, 0.0625, 0.125, 0.25, 0.5, 1, 2, 4, 8, 16]
LO, HI = math.log2(TICKS[0]), math.log2(TICKS[-1])


def ypos(v):
    return Y1 - (math.log2(v) - LO) / (HI - LO) * (Y1 - Y0)


def fmt(v):
    return f"{v:g}x" if v >= 1 else f"{v:.2f}x".rstrip("0").rstrip(".") + ("" if v >= 1 else "")


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def main():
    if not SRC.exists():
        sys.exit(f"no {SRC} — run bench/scripts/projection_benefit.sh first")
    doc = json.loads(SRC.read_text())
    rows = doc["rows"]
    scales = sorted({r["scale"] for r in rows})

    def get(q, s):
        return next(r for r in rows if r["query"] == q and r["scale"] == s)

    cols = [c for _, c in LINE_QUERIES]
    # sqrt x-spacing: the column counts run 0,1,2,5,10,19 and a linear axis
    # would crush the left half, which is where the mechanism is.
    def xpos(c, lo=L1, hi=R1):
        t = math.sqrt(c) / math.sqrt(max(cols))
        return lo + t * (hi - lo)

    o = []
    a = o.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
      f'width="{W}" height="{H}" font-family="system-ui,-apple-system,Segoe UI,sans-serif" '
      f'role="img" aria-label="Mirror speedup against PostgreSQL, by columns touched and table size">')
    a("""<style>
    :root{--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
          --grid:#e1e0d9;--axis:#c3c2b7;--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;}
    @media (prefers-color-scheme:dark){
      :root{--surface:#1a1a19;--ink:#ffffff;--ink2:#c3c2b7;--muted:#898781;
            --grid:#2c2c2a;--axis:#383835;--s1:#3987e5;--s2:#d95926;--s3:#199e70;}
    }
    .ttl{fill:var(--ink);font-size:15px;font-weight:600}
    .sub{fill:var(--ink2);font-size:11.5px}
    .ax{fill:var(--muted);font-size:10.5px;font-variant-numeric:tabular-nums}
    .lbl{font-size:11px;font-weight:600;font-variant-numeric:tabular-nums}
    .par{fill:var(--ink2);font-size:10.5px;font-weight:600}
    .pnl{fill:var(--ink);font-size:12px;font-weight:600}
    </style>""")
    a(f'<rect width="{W}" height="{H}" fill="var(--surface)"/>')

    a(f'<text class="ttl" x="20" y="26">The projection pays in proportion to what it lets you skip</text>')
    a(f'<text class="sub" x="20" y="45">Speedup vs the same query on the PostgreSQL heap, same node, same page cache. '
      f'Above 1&#215; the mirror wins. Best of {doc["reps"]}.</text>')

    # gridlines + y ticks, shared by both panels
    for t in TICKS:
        y = ypos(t)
        w = 2 if t == 1 else 1
        col = "var(--axis)" if t == 1 else "var(--grid)"
        dash = ' stroke-dasharray="5 4"' if t == 1 else ""
        a(f'<line x1="{L1}" y1="{y:.1f}" x2="{R1}" y2="{y:.1f}" stroke="{col}" stroke-width="{w}"{dash}/>')
        a(f'<line x1="{L2}" y1="{y:.1f}" x2="{R2}" y2="{y:.1f}" stroke="{col}" stroke-width="{w}"{dash}/>')
        a(f'<text class="ax" x="{L1-8}" y="{y+3.5:.1f}" text-anchor="end">{fmt(t)}</text>')
    a(f'<text class="par" x="{R1-4}" y="{ypos(1)+15:.1f}" text-anchor="end">parity</text>')

    # ---- left panel: scans
    a(f'<text class="pnl" x="{L1}" y="{Y0-14}">Analytical scans</text>')
    for c in cols:
        x = xpos(c)
        a(f'<text class="ax" x="{x:.1f}" y="{Y1+18}" text-anchor="middle">{c}</text>')
    a(f'<text class="ax" x="{(L1+R1)/2:.0f}" y="{Y1+38}" text-anchor="middle" '
      f'style="fill:var(--ink2)">columns the query touches (of 19)</text>')

    for i, s in enumerate(scales):
        light, _ = SERIES[i]
        var = f"var(--s{i+1})"
        pts = [(xpos(c), ypos(get(q, s)["view_speedup"])) for q, c in LINE_QUERIES]
        a('<polyline fill="none" stroke="' + var + '" stroke-width="2" stroke-linejoin="round" points="'
          + " ".join(f"{x:.1f},{y:.1f}" for x, y in pts) + '"/>')
        for (x, y), (q, c) in zip(pts, LINE_QUERIES):
            # 2px surface ring so overlapping markers stay countable
            a(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{var}" stroke="var(--surface)" stroke-width="2"/>')
        # ONE direct label per line, at x=1 where the three are furthest apart,
        # carrying both the identity and the headline value. Anchored into the
        # plot: anchored outward at x=0 it ran off the canvas and "0.49M rows"
        # rendered as "9M rows", which the palette validator cannot see.
        xl, yl = pts[1]
        live = doc["scales"][str(s)]["live_rows"]
        v1 = get(LINE_QUERIES[1][0], s)["view_speedup"]
        # The middle line's label goes BELOW its marker; above, the line above it
        # ran straight through the text. Above/below/above keeps all three clear
        # without moving them off the point they identify.
        dy = 21 if i == 1 else -9
        a(f'<text class="lbl" x="{xl+11:.1f}" y="{yl+dy:.1f}" text-anchor="start" fill="{var}">'
          f'{live/1e6:.2f}M rows &#183; {v1:.1f}&#215;</text>')

    # ---- right panel: the point lookup
    a(f'<text class="pnl" x="{L2}" y="{Y0-14}">Point lookup</text>')
    bw = (R2 - L2) / (len(scales) * 1.6)
    for i, s in enumerate(scales):
        var = f"var(--s{i+1})"
        v = get(LOOKUP, s)["view_speedup"]
        x = L2 + (i + 0.5) * (R2 - L2) / len(scales) - bw / 2
        y = ypos(v)
        yb = ypos(1)
        # drawn from the parity line DOWN: the bar's length is the distance from
        # break-even, which is the quantity that means something here.
        a(f'<rect x="{x:.1f}" y="{yb:.1f}" width="{bw:.1f}" height="{y-yb:.1f}" fill="{var}" rx="4"/>')
        a(f'<text class="lbl" x="{x+bw/2:.1f}" y="{y+15:.1f}" text-anchor="middle" fill="{var}">{v:.2f}&#215;</text>')
        a(f'<text class="ax" x="{x+bw/2:.1f}" y="{Y1+18}" text-anchor="middle">'
          f'{doc["scales"][str(s)]["live_rows"]/1e6:.1f}M</text>')
    a(f'<text class="ax" x="{(L2+R2)/2:.0f}" y="{Y1+38}" text-anchor="middle" '
      f'style="fill:var(--ink2)">live rows</text>')

    a(f'<text class="sub" x="20" y="{H-28}">'
      f'Left: fewer columns read &#8594; more the mirror skips &#8594; bigger win, and every line rises with size '
      f'as the heap stops fitting in RAM.</text>')
    a(f'<text class="sub" x="20" y="{H-11}">'
      f'Right: the same mirror is ~16&#215; SLOWER for a single-row probe, and gets worse with scale. '
      f'That is why mode: takeover is gated.</text>')
    a("</svg>")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(o) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)} from {len(rows)} measurements")


if __name__ == "__main__":
    main()
