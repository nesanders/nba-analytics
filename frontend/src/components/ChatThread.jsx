// Scrollable message list. Shows clickable example prompts when empty.
// Auto-scrolls to the bottom on new messages and while the loading indicator
// is visible.
import { useEffect, useRef } from 'react'
import Message from './Message.jsx'
import styles from './ChatThread.module.css'

const EXAMPLES = [
  'Who led the league in scoring in 2023-24?',
  "Show LeBron James's points per game by season",
  "Compare Curry and Thompson's 3PT% by season",
  'Which teams had the best offensive rating in 2022-23?',
  'Who leads all time in pts per 36 min through age 28, min 2000 minutes?',
]

export default function ChatThread({ messages, loading, onSend, onArtifactClick }) {
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  if (messages.length === 0) {
    return (
      <div className={styles.empty}>
        <p className={styles.emptyTitle}>Ask anything about NBA stats</p>
        <div className={styles.examples}>
          {EXAMPLES.map(ex => (
            <button key={ex} className={styles.example} onClick={() => onSend(ex)}>
              {ex}
            </button>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className={styles.thread}>
      {messages.map((msg, i) => (
        <Message key={i} message={msg} onArtifactClick={onArtifactClick} />
      ))}
      {loading && (
        <div className={styles.thinking}>
          <span className={styles.dot} />
          <span className={styles.dot} />
          <span className={styles.dot} />
        </div>
      )}
      <div ref={bottomRef} />
    </div>
  )
}
