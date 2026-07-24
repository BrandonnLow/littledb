package lincheck

import (
	"fmt"
	"sort"
	"strings"
)

// RenderHistoryHTML renders a recorded history as a self-contained HTML page: a
// real-time timeline with one row per client, each operation a bar spanning its
// [call, return] window (an unfinished or fate-unknown Infinity return runs to the
// right edge, capped with an arrow). Bars are colored by key; when res reports a
// violation, the offending key's operations — res.Witness — are outlined in red to
// mark the counterexample. The page is dependency-free (inline CSS + SVG) and
// adapts to the viewer's light/dark theme.
//
// It is a diagnosis aid, not part of the checker: staring at a text history to
// understand why a run was non-linearizable is painful; a timeline makes the
// offending read legible at a glance.
func RenderHistoryHTML(ops []Op, res Result) string {
	const (
		leftPad  = 70.0
		rightPad = 24.0
		topPad   = 16.0
		rowH     = 26.0
		rowGap   = 8.0
		plotW    = 1120.0
	)

	// Distinct clients (rows) and keys (colors), in stable order.
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

	// Which (client, call, return) triples are in the witness, for highlighting.
	witness := map[string]bool{}
	for _, op := range res.Witness {
		witness[opID(op)] = true
	}

	xOf := func(tick int64) float64 {
		return leftPad + float64(tick-minTick)/float64(maxTick-minTick)*plotW
	}
	rightEdge := leftPad + plotW
	height := topPad*2 + float64(len(clients))*(rowH+rowGap)
	width := leftPad + plotW + rightPad

	var b strings.Builder
	fmt.Fprintf(&b, "<svg viewBox=\"0 0 %.0f %.0f\" xmlns=\"http://www.w3.org/2000/svg\" class=\"lincheck-timeline\" role=\"img\">\n", width, height)

	// Row baselines + client labels.
	for _, c := range clients {
		y := topPad + float64(rowOf[c])*(rowH+rowGap)
		fmt.Fprintf(&b, "  <text x=\"%.0f\" y=\"%.1f\" class=\"rowlabel\">c%d</text>\n", 12.0, y+rowH*0.65, c)
		fmt.Fprintf(&b, "  <line x1=\"%.0f\" y1=\"%.1f\" x2=\"%.0f\" y2=\"%.1f\" class=\"grid\"/>\n", leftPad, y+rowH+rowGap/2, rightEdge, y+rowH+rowGap/2)
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
		fill := colorOf[op.Key]
		cls := "bar"
		if op.Kind == Get {
			cls = "bar read"
		}
		if witness[opID(op)] {
			cls += " witness"
		}
		title := opLabel(op)
		fmt.Fprintf(&b, "  <g class=\"%s\"><title>%s</title>", cls, esc(title))
		fmt.Fprintf(&b, "<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"3\" fill=\"%s\"/>",
			x1, y, x2-x1, rowH, fill)
		if unbounded {
			// Arrow marking the run-to-the-edge (Infinity) return.
			fmt.Fprintf(&b, "<polygon points=\"%.1f,%.1f %.1f,%.1f %.1f,%.1f\" class=\"inf\"/>",
				x2, y+rowH/2-5, x2+8, y+rowH/2, x2, y+rowH/2+5)
		}
		if x2-x1 > 34 { // label only when the bar is wide enough to read
			fmt.Fprintf(&b, "<text x=\"%.1f\" y=\"%.1f\" class=\"barlabel\">%s</text>", x1+4, y+rowH*0.66, esc(shortLabel(op)))
		}
		b.WriteString("</g>\n")
	}
	b.WriteString("</svg>")

	verdict := "linearizable"
	verdictClass := "ok"
	if !res.Linearizable {
		verdict = fmt.Sprintf("NOT linearizable — offending key %q", res.Key)
		verdictClass = "bad"
	}

	var legend strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&legend, "<span class=\"chip\"><i style=\"background:%s\"></i>%s</span>", colorOf[k], esc(k))
	}

	return fmt.Sprintf(htmlShell, verdictClass, esc(verdict), len(ops), legend.String(), b.String())
}

// palette is a brand-neutral set of hues with adequate contrast on both light and
// dark backgrounds; keys cycle through it.
var palette = []string{
	"#4c78a8", "#f58518", "#54a24b", "#b279a2",
	"#e45756", "#72b7b2", "#eeca3b", "#9d755d",
}

func opID(op Op) string {
	return fmt.Sprintf("%d:%d:%d", op.Client, op.Call, op.Return)
}

func opLabel(op Op) string {
	ret := fmt.Sprintf("%d", op.Return)
	if op.Return == Infinity {
		ret = "∞"
	}
	switch op.Kind {
	case Put:
		return fmt.Sprintf("c%d put %s=%s  [%d..%s]", op.Client, op.Key, op.Value, op.Call, ret)
	case Delete:
		return fmt.Sprintf("c%d del %s  [%d..%s]", op.Client, op.Key, op.Call, ret)
	default:
		if op.Found {
			return fmt.Sprintf("c%d get %s → %s  [%d..%s]", op.Client, op.Key, op.Value, op.Call, ret)
		}
		return fmt.Sprintf("c%d get %s → ∅  [%d..%s]", op.Client, op.Key, op.Call, ret)
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
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;")
	return r.Replace(s)
}

// htmlShell wraps the SVG. Placeholders: verdict class, verdict text, op count,
// legend HTML, svg. Theme-aware via prefers-color-scheme.
const htmlShell = `<!doctype html>
<html><head><meta charset="utf-8"><title>lincheck history</title>
<style>
  :root { --bg:#fff; --fg:#1a1a1a; --muted:#666; --grid:#eee; --card:#f7f7f8; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#161618; --fg:#e8e8ea; --muted:#9a9aa2; --grid:#2a2a2e; --card:#1e1e22; }
  }
  body { margin:0; padding:20px; background:var(--bg); color:var(--fg);
         font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif; }
  h1 { font-size:16px; margin:0 0 4px; }
  .verdict { font-weight:600; margin-bottom:12px; }
  .verdict.ok { color:#2e9e56; }
  .verdict.bad { color:#e45756; }
  .meta { color:var(--muted); font-size:12px; margin-bottom:12px; }
  .legend { margin:10px 0 4px; }
  .chip { display:inline-flex; align-items:center; margin-right:12px; font-size:12px; color:var(--muted); }
  .chip i { width:11px; height:11px; border-radius:2px; margin-right:5px; display:inline-block; }
  .wrap { overflow-x:auto; background:var(--card); border-radius:8px; padding:8px 4px; }
  svg { display:block; min-width:900px; }
  .rowlabel { fill:var(--muted); font-size:12px; }
  .barlabel { fill:#fff; font-size:11px; pointer-events:none; }
  .grid { stroke:var(--grid); stroke-width:1; }
  .bar rect { opacity:.92; }
  .bar.read rect { opacity:.62; }
  .bar.witness rect { stroke:#e5484d; stroke-width:2.5; }
  .inf { fill:var(--muted); }
</style></head>
<body>
  <h1>lincheck history</h1>
  <div class="verdict %s">%s</div>
  <div class="meta">%d operations · time →, one row per client · bars span [call, return], ∞ = unfinished/uncertain</div>
  <div class="legend">%s</div>
  <div class="wrap">%s</div>
</body></html>`
