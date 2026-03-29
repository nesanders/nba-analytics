import { useState } from 'react'
import PlotlyChart from './PlotlyChart.jsx'
import styles from './Message.module.css'

export default function Message({ message }) {
  const [showSql, setShowSql] = useState(false)
  const isUser = message.role === 'user'

  return (
    <div className={`${styles.wrapper} ${isUser ? styles.user : styles.assistant}`}>
      <div className={`${styles.bubble} ${message.error ? styles.error : ''}`}>
        <p className={styles.text}>{message.content}</p>

        {message.figure && (
          <div className={styles.chart}>
            <PlotlyChart figure={message.figure} />
          </div>
        )}

        {message.sql && (
          <div className={styles.sqlSection}>
            <button
              className={styles.sqlToggle}
              onClick={() => setShowSql(v => !v)}
            >
              {showSql ? 'Hide SQL' : 'Show SQL'}
            </button>
            {showSql && (
              <pre className={styles.sql}>{message.sql}</pre>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
