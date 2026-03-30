// Right-hand artifact tray. Shows charts and SQL produced by each assistant
// response. Each card is collapsible; the newest card auto-expands when it arrives.
// Clicking ⤢ on a card header opens the full-page overlay.
// Clicking an artifact link in chat scrolls here, flashes the card, and opens the overlay.
import { useState, useEffect } from 'react'
import { format as formatSql } from 'sql-formatter'
import PlotlyChart from './PlotlyChart.jsx'
import styles from './ArtifactTray.module.css'

export default function ArtifactTray({ artifacts, focusedArtifactId, highlightedArtifactId, onFocusChange }) {
  const [expandedCardId, setExpandedCardId] = useState(null)
  const focusedArtifact = artifacts.find(a => a.id === focusedArtifactId) ?? null

  // Auto-expand the newest artifact when it arrives; collapse all others
  useEffect(() => {
    if (artifacts.length > 0) {
      setExpandedCardId(artifacts[artifacts.length - 1].id)
    }
  }, [artifacts.length])

  const handleToggleExpand = (id) => {
    setExpandedCardId(prev => (prev === id ? null : id))
  }

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
                expanded={artifact.id === expandedCardId}
                onToggleExpand={() => handleToggleExpand(artifact.id)}
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

function ArtifactCard({ artifact, highlighted, expanded, onToggleExpand, onFocus }) {
  return (
    <div
      id={`artifact-${artifact.id}`}
      className={`${styles.card} ${highlighted ? styles.highlighted : ''}`}
    >
      <div className={styles.cardHeader}>
        <button className={styles.collapseBtn} onClick={onToggleExpand} title={expanded ? 'Collapse' : 'Expand'}>
          <span className={styles.badge}>#{artifact.id}</span>
          <span className={styles.collapseIcon}>{expanded ? '▾' : '▸'}</span>
          <span className={styles.question}>{artifact.question}</span>
        </button>
        <button className={styles.expandBtn} onClick={onFocus} title="Open full view">⤢</button>
      </div>
      {expanded && (
        <>
          {artifact.figure && (
            <div className={styles.chart}>
              <PlotlyChart figure={artifact.figure} />
            </div>
          )}
          {artifact.sql && <SqlBlock sql={artifact.sql} />}
        </>
      )}
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
