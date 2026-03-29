// Right-hand artifact tray. Shows charts and SQL produced by each assistant
// response. Each card is numbered to match the badge shown in the chat bubble.
// Clicking a card header focuses it in a full-page overlay.
import { useState } from 'react'
import { format as formatSql } from 'sql-formatter'
import PlotlyChart from './PlotlyChart.jsx'
import styles from './ArtifactTray.module.css'

export default function ArtifactTray({ artifacts }) {
  const [focused, setFocused] = useState(null)

  const focusedArtifact = artifacts.find(a => a.id === focused)

  return (
    <>
      <div className={styles.tray}>
        <div className={styles.header}>
          <span className={styles.title}>Artifacts</span>
          {artifacts.length > 0 && <span className={styles.count}>{artifacts.length}</span>}
        </div>
        {artifacts.length === 0 ? (
          <div className={styles.empty}>
            <div className={styles.emptyIcon}>📊</div>
            <p>Charts and SQL will appear here</p>
          </div>
        ) : (
          <div className={styles.list}>
            {artifacts.map(artifact => (
              <ArtifactCard
                key={artifact.id}
                artifact={artifact}
                onFocus={() => setFocused(artifact.id)}
              />
            ))}
          </div>
        )}
      </div>

      {focusedArtifact && (
        <div className={styles.overlay} onClick={() => setFocused(null)}>
          <div className={styles.overlayCard} onClick={e => e.stopPropagation()}>
            <div className={styles.overlayHeader}>
              <span className={styles.badge}>#{focusedArtifact.id}</span>
              <span className={styles.overlayQuestion}>{focusedArtifact.question}</span>
              <button className={styles.closeBtn} onClick={() => setFocused(null)}>✕</button>
            </div>
            {focusedArtifact.figure && (
              <div className={styles.overlayChart}>
                <PlotlyChart figure={focusedArtifact.figure} />
              </div>
            )}
            {focusedArtifact.sql && <SqlBlock sql={focusedArtifact.sql} defaultOpen />}
          </div>
        </div>
      )}
    </>
  )
}

function ArtifactCard({ artifact, onFocus }) {
  return (
    <div id={`artifact-${artifact.id}`} className={styles.card}>
      <button className={styles.cardHeader} onClick={onFocus} title="Click to expand">
        <span className={styles.badge}>#{artifact.id}</span>
        <span className={styles.question}>{artifact.question}</span>
        <span className={styles.expandHint}>⤢</span>
      </button>
      {artifact.figure && (
        <div className={styles.chart}>
          <PlotlyChart figure={artifact.figure} />
        </div>
      )}
      {artifact.sql && <SqlBlock sql={artifact.sql} />}
    </div>
  )
}

function SqlBlock({ sql, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen)

  let pretty = sql
  try {
    pretty = formatSql(sql, { language: 'sql', keywordCase: 'upper', tabWidth: 2 })
  } catch {
    // fall back to raw SQL if formatter chokes
  }

  return (
    <div className={styles.sqlSection}>
      <button className={styles.sqlToggle} onClick={() => setOpen(v => !v)}>
        {open ? 'Hide SQL' : 'Show SQL'}
      </button>
      {open && <pre className={styles.sql}>{pretty}</pre>}
    </div>
  )
}
