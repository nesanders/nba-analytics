// Thin wrapper around Plotly.react(). Lazily imports plotly.js-dist-min on
// first render to avoid blocking the initial page load. Calls Plotly.react
// (not Plotly.newPlot) so React re-renders update the chart in place.
import { useEffect, useRef } from 'react'

// Load Plotly from CDN-style import via the dist min bundle
// This avoids bundling the full 3MB Plotly into the app
let Plotly = null

async function getPlotly() {
  if (!Plotly) {
    Plotly = (await import('plotly.js-dist-min')).default
  }
  return Plotly
}

export default function PlotlyChart({ figure }) {
  const divRef = useRef(null)

  useEffect(() => {
    if (!figure || !divRef.current) return

    let cancelled = false
    getPlotly().then((P) => {
      if (cancelled || !divRef.current) return
      P.react(divRef.current, figure.data, {
        ...figure.layout,
        autosize: true,
      }, { responsive: true, displayModeBar: true, displaylogo: false })
    })

    return () => { cancelled = true }
  }, [figure])

  return <div ref={divRef} style={{ width: '100%', minHeight: 300 }} />
}
