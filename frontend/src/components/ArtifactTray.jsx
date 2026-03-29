// Right-hand artifact tray. Shows charts and SQL produced by each assistant
// response. Each card is numbered to match the badge shown in the chat bubble.
// Clicking a badge in the chat scrolls to the corresponding card here.
import { useState } from 'react'
import { format as formatSql } from 'sql-formatter'
import PlotlyChart from './PlotlyChart.jsx'
import styles from './ArtifactTray.module.css'

export default function ArtifactTray({ artifacts }) {
  if (artifacts.length === 0) {
    return (
      <div className={styles.tray}>
        <div className={styles.header}>
          <span className={styles.title}>Artifacts</span>
        </div>
        <div className={styles.empty}>
          <div className={styles.emptyIcon}>📊</div>
          <p>Charts and SQL will appear here</p>
        </div>
      </div>
    )
  }

  return (
    <div className={styles.tray}>
      <div className={styles.header}>
        <span className={styles.title}>Artifacts</span>
        <span className={styles.count}>{artifacts.length}</span>
      </div>
      <div className={styles.list}>
        {artifacts.map(artifact => (
          <ArtifactCard key={artifact.id} artifact={artifact} />
        ))}
      </div>
    </div>
  )
}

function ArtifactCard({ artifact }) {
  return (
    <div id={`artifact-${artifact.id}`} className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.badge}>#{artifact.id}</span>
        <span className={styles.question} title={artifact.question}>
          {artifact.question}
        </span>
      </div>
      {artifact.figure && (
        <div className={styles.chart}>
          <PlotlyChart figure={artifact.figure} />
        </div>
      )}
      {artifact.sql && <SqlBlock sql={artifact.sql} />}
    </div>
  )
}

function SqlBlock({ sql }) {
  const [open, setOpen] = useState(false)

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
