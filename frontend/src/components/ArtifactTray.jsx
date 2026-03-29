// Right-hand artifact tray. Shows charts and SQL produced by each assistant
// response. Each card is numbered to match the badge shown in the chat bubble.
// Clicking a card header opens a full-page overlay. Clicking an artifact link
// in chat scrolls here, flashes the card, and opens the overlay.
import { useState } from 'react'
import { format as formatSql } from 'sql-formatter'
import PlotlyChart from './PlotlyChart.jsx'
import styles from './ArtifactTray.module.css'

export default function ArtifactTray({ artifacts, focusedArtifactId, highlightedArtifactId, onFocusChange }) {
  const focusedArtifact = artifacts.find(a => a.id === focusedArtifactId) ?? null

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
                highlighted={artifact.id === highlightedArtifactId}
                onFocus={() => onFocusChange(artifact.id)}
              />
            ))}
          </div>
        )}
      </div>

      {focusedArtifact && (
        <div className={styles.overlay} onClick={() => onFocusChange(null)}>
          <div className={styles.overlayCard} onClick={e => e.stopPropagation()}>
            <div className={styles.overlayHeader}>
              <span className={styles.badge}>#{focusedArtifact.id}</span>
              <span className={styles.overlayQuestion}>{focusedArtifact.question}</span>
              <button className={styles.closeBtn} onClick={() => onFocusChange(null)}>✕</button>
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

function ArtifactCard({ artifact, highlighted, onFocus }) {
  return (
    <div
      id={`artifact-${artifact.id}`}
      className={`${styles.card} ${highlighted ? styles.highlighted : ''}`}
    >
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
