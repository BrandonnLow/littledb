package lincheck

import (
	"fmt"
	"sort"
	"strings"
)

// RenderHistoryHTML renders a recorded history as a full, self-contained HTML
// page. See RenderHistoryFragment for the embeddable body it wraps.
func RenderHistoryHTML(ops []Op, res Result) string {
	return "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">" +
		"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" +
		"<title>lincheck history</title></head>\n<body style=\"margin:0;padding:20px\">\n" +
		RenderHistoryFragment(ops, res) + "\n</body></html>\n"
}

// RenderHistoryFragment renders a recorded history as a self-contained, embeddable
// block: scoped styles plus a real-time swimlane timeline — one lane per client,
// each operation a bar spanning its [call, return] window (an unfinished or
// fate-unknown Infinity return runs to the right edge, capped with an arrow), a
// faint time axis, and a verdict header. Bars are colored by key; when res reports
// a violation, the offending key's operations — res.Witness — are outlined to mark
// the counterexample. It adapts to the viewer's light/dark theme.
//
// It is a diagnosis aid, not part of the checker: staring at a text history to see
// why a run was non-linearizable is painful; a timeline makes the offending read
// legible at a glance.
func RenderHistoryFragment(ops []Op, res Result) string {
	const (
		leftPad  = 78.0
		rightPad = 24.0
		topPad   = 14.0
		rowH     = 26.0
		rowGap   = 8.0
		axisH    = 26.0
		plotW    = 1080.0
	)

	clientSet := map[int]bool{}
	keySet := map[string]bool{}
	minTick, maxTick := int64(1<<62), int64(0)
	for _, op := range ops {
		clientSet[op.Client] = true
		keySet[op.Key] = true
		if op.Call < minTick {
			minTick = op.Call
		}
		if op.Return != Infinity && op.Return > maxTick {
			maxTick = op.Return
		}
		if op.Call > maxTick {
			maxTick = op.Call
		}
	}
	if len(ops) == 0 {
		minTick, maxTick = 0, 1
	}
	if maxTick <= minTick {
		maxTick = minTick + 1
	}
	clients := sortedInts(clientSet)
	keys := sortedStrings(keySet)
	colorOf := map[string]string{}
	for i, k := range keys {
		colorOf[k] = palette[i%len(palette)]
	}
	rowOf := map[int]int{}
	for i, c := range clients {
		rowOf[c] = i
	}
	witness := map[string]bool{}
	for _, op := range res.Witness {
		witness[opID(op)] = true
	}

	xOf := func(tick int64) float64 {
		return leftPad + float64(tick-minTick)/float64(maxTick-minTick)*plotW
	}
	rightEdge := leftPad + plotW
	rowsBottom := topPad + float64(len(clients))*(rowH+rowGap)
	height := rowsBottom + axisH
	width := leftPad + plotW + rightPad

	var b strings.Builder
	fmt.Fprintf(&b, "<svg viewBox=\"0 0 %.0f %.0f\" xmlns=\"http://www.w3.org/2000/svg\" role=\"img\" aria-label=\"operation timeline\">\n", width, height)

	// Time axis: faint vertical gridlines with monospace tick labels along the
	// bottom, so the lanes read as a shared clock rather than free-floating bars.
	const nTicks = 6
	for i := 0; i < nTicks; i++ {
		tick := minTick + int64(float64(i)/(nTicks-1)*float64(maxTick-minTick))
		x := xOf(tick)
		fmt.Fprintf(&b, "  <line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" class=\"grid\"/>\n", x, topPad, x, rowsBottom)
		anchor := "middle"
		if i == 0 {
			anchor = "start"
		} else if i == nTicks-1 {
			anchor = "end"
		}
		fmt.Fprintf(&b, "  <text x=\"%.1f\" y=\"%.1f\" text-anchor=\"%s\" class=\"axislabel\">%d</text>\n", x, rowsBottom+16, anchor, tick)
	}
	fmt.Fprintf(&b, "  <line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" class=\"axis\"/>\n", leftPad, rowsBottom, rightEdge, rowsBottom)

	// Lane labels.
	for _, c := range clients {
		y := topPad + float64(rowOf[c])*(rowH+rowGap)
		fmt.Fprintf(&b, "  <text x=\"%.0f\" y=\"%.1f\" class=\"rowlabel\">c%d</text>\n", 14.0, y+rowH*0.66, c)
	}

	// Operation bars.
	for _, op := range ops {
		y := topPad + float64(rowOf[op.Client])*(rowH+rowGap)
		x1 := xOf(op.Call)
		x2 := rightEdge
		unbounded := op.Return == Infinity
		if !unbounded {
			x2 = xOf(op.Return)
		}
		if x2 < x1+3 {
			x2 = x1 + 3
		}
		cls := "bar"
		if op.Kind == Get {
			cls = "bar read"
		}
		if witness[opID(op)] {
			cls += " witness"
		}
		fmt.Fprintf(&b, "  <g class=\"%s\"><title>%s</title>", cls, esc(opLabel(op)))
		fmt.Fprintf(&b, "<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"4\" fill=\"%s\"/>",
			x1, y, x2-x1, rowH, colorOf[op.Key])
		if unbounded {
			fmt.Fprintf(&b, "<polygon points=\"%.1f,%.1f %.1f,%.1f %.1f,%.1f\" class=\"inf\"/>",
				x2, y+rowH/2-5, x2+8, y+rowH/2, x2, y+rowH/2+5)
		}
		if x2-x1 > 38 {
			fmt.Fprintf(&b, "<text x=\"%.1f\" y=\"%.1f\" class=\"barlabel\">%s</text>", x1+5, y+rowH*0.66, esc(shortLabel(op)))
		}
		b.WriteString("</g>\n")
	}
	b.WriteString("</svg>")

	pillClass, pill := "ok", "linearizable"
	caption := "Each lane is a client; time runs left → right; every bar spans an operation's [call, return] window (∞ = unfinished / fate-unknown). Every completed read is consistent with some single-machine ordering."
	if !res.Linearizable {
		pillClass = "bad"
		pill = fmt.Sprintf("not linearizable · key %q", res.Key)
		caption = fmt.Sprintf("Each lane is a client; time runs left → right; every bar spans an operation's [call, return] window (∞ = unfinished / fate-unknown). The <b>outlined</b> operations on key %q are the counterexample — a read observed a value that no ordering consistent with real time allows.", res.Key)
	}

	var legend strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&legend, "<span class=\"chip\"><i style=\"background:%s\"></i>%s</span>", colorOf[k], esc(k))
	}

	return fmt.Sprintf(fragmentShell, esc(pill), pillClass, pill, len(ops), caption, legend.String(), b.String())
}

// palette is a categorical set with adequate contrast on both light and dark
// grounds; red is deliberately excluded so it can mark violations exclusively.
var palette = []string{
	"#4c78a8", "#f58518", "#54a24b", "#b279a2",
	"#72b7b2", "#b6992d", "#e377c2", "#6a7b8a",
}

func opID(op Op) string { return fmt.Sprintf("%d:%d:%d", op.Client, op.Call, op.Return) }

func opLabel(op Op) string {
	ret := fmt.Sprintf("%d", op.Return)
	if op.Return == Infinity {
		ret = "∞"
	}
	switch op.Kind {
	case Put:
		return fmt.Sprintf("c%d  put %s=%s   [%d..%s]", op.Client, op.Key, op.Value, op.Call, ret)
	case Delete:
		return fmt.Sprintf("c%d  del %s   [%d..%s]", op.Client, op.Key, op.Call, ret)
	default:
		if op.Found {
			return fmt.Sprintf("c%d  get %s → %s   [%d..%s]", op.Client, op.Key, op.Value, op.Call, ret)
		}
		return fmt.Sprintf("c%d  get %s → ∅   [%d..%s]", op.Client, op.Key, op.Call, ret)
	}
}

func shortLabel(op Op) string {
	switch op.Kind {
	case Put:
		return "put " + op.Value
	case Delete:
		return "del"
	default:
		if op.Found {
			return "get→" + op.Value
		}
		return "get→∅"
	}
}

func sortedInts(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

func sortedStrings(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;").Replace(s)
}

// fragmentShell scopes every rule to .lincheck so the block is safe to embed, and
// carries its own light/dark tokens (honoring both prefers-color-scheme and a host
// data-theme toggle). Placeholders: pill aria text, pill class, pill text, op
// count, caption, legend, svg.
const fragmentShell = `<style>
  .lincheck { --fg:#14161a; --muted:#5b6270; --faint:#8b91a0; --grid:#e9ebf0;
    --card:#f6f7f9; --line:#e2e5ea; --ok:#1f9d57; --ok-bg:#e7f6ed; --bad:#e5484d; --bad-bg:#fdecec;
    color:var(--fg); font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
    -webkit-font-smoothing:antialiased; }
  @media (prefers-color-scheme:dark) {
    .lincheck { --fg:#e6e8ec; --muted:#8b93a1; --faint:#5f6772; --grid:#20242c;
      --card:#15181e; --line:#262b34; --ok:#3fce80; --ok-bg:#12291d; --bad:#ff6b6f; --bad-bg:#2b1416; }
  }
  :root[data-theme="dark"] .lincheck { --fg:#e6e8ec; --muted:#8b93a1; --faint:#5f6772; --grid:#20242c;
    --card:#15181e; --line:#262b34; --ok:#3fce80; --ok-bg:#12291d; --bad:#ff6b6f; --bad-bg:#2b1416; }
  :root[data-theme="light"] .lincheck { --fg:#14161a; --muted:#5b6270; --faint:#8b91a0; --grid:#e9ebf0;
    --card:#f6f7f9; --line:#e2e5ea; --ok:#1f9d57; --ok-bg:#e7f6ed; --bad:#e5484d; --bad-bg:#fdecec; }
  .lincheck .head { display:flex; align-items:baseline; gap:12px; flex-wrap:wrap; margin-bottom:10px; }
  .lincheck .title { font:600 12px ui-monospace,SFMono-Regular,Menlo,monospace; letter-spacing:.12em;
    text-transform:uppercase; color:var(--muted); margin:0; }
  .lincheck .pill { font:600 12px ui-monospace,SFMono-Regular,Menlo,monospace; padding:3px 10px; border-radius:999px; }
  .lincheck .pill.ok { color:var(--ok); background:var(--ok-bg); }
  .lincheck .pill.bad { color:var(--bad); background:var(--bad-bg); }
  .lincheck .count { font:12px ui-monospace,SFMono-Regular,Menlo,monospace; color:var(--faint);
    font-variant-numeric:tabular-nums; }
  .lincheck .caption { color:var(--muted); font-size:13px; line-height:1.55; margin:0 0 14px; max-width:74ch; }
  .lincheck .caption b { color:var(--fg); font-weight:600; }
  .lincheck .legend { display:flex; flex-wrap:wrap; gap:14px; margin:12px 2px 4px; }
  .lincheck .chip { display:inline-flex; align-items:center; gap:6px; color:var(--muted);
    font:12px ui-monospace,SFMono-Regular,Menlo,monospace; }
  .lincheck .chip i { width:11px; height:11px; border-radius:3px; }
  .lincheck .wrap { overflow-x:auto; background:var(--card); border:1px solid var(--line);
    border-radius:10px; padding:10px 8px; }
  .lincheck svg { display:block; min-width:780px; }
  .lincheck .rowlabel { fill:var(--muted); font:600 12px ui-monospace,SFMono-Regular,Menlo,monospace; }
  .lincheck .axislabel { fill:var(--faint); font:11px ui-monospace,SFMono-Regular,Menlo,monospace;
    font-variant-numeric:tabular-nums; }
  .lincheck .barlabel { fill:#fff; font:11px ui-monospace,SFMono-Regular,Menlo,monospace; pointer-events:none; }
  .lincheck .grid { stroke:var(--grid); stroke-width:1; }
  .lincheck .axis { stroke:var(--line); stroke-width:1; }
  .lincheck .bar.read rect { opacity:.6; }
  .lincheck .bar.witness rect { stroke:var(--bad); stroke-width:2.5; }
  .lincheck .inf { fill:var(--faint); }
</style>
<div class="lincheck" role="figure" aria-label="lincheck timeline: %s">
  <div class="head">
    <p class="title">lincheck · history</p>
    <span class="pill %s">%s</span>
    <span class="count">%d ops</span>
  </div>
  <p class="caption">%s</p>
  <div class="legend">%s</div>
  <div class="wrap">%s</div>
</div>`
