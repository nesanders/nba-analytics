// Full-screen modal shown on first visit or after key rejection.
// Validates that the key starts with "gsk_" client-side, then calls onKeySet
// which persists it to localStorage. The key is never sent to our servers.
import { useState } from 'react'
import styles from './ApiKeyModal.module.css'

export default function ApiKeyModal({ onKeySet }) {
  const [key, setKey] = useState('')
  const [error, setError] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    const trimmed = key.trim()
    if (!trimmed.startsWith('gsk_')) {
      setError('Groq API keys start with "gsk_". Get one free at console.groq.com.')
      return
    }
    onKeySet(trimmed)
  }

  return (
    <div className={styles.overlay}>
      <div className={styles.modal}>
        <h1 className={styles.title}>🏀 NBA Analytics</h1>
        <p className={styles.desc}>
          This app uses your own{' '}
          <a href="https://console.groq.com/keys" target="_blank" rel="noreferrer">
            Groq API key
          </a>{' '}
          (free) to answer NBA questions with AI-generated SQL and charts.
          Your key is stored only in your browser.
        </p>
        <form onSubmit={handleSubmit} className={styles.form}>
          <input
            className={styles.input}
            type="password"
            placeholder="gsk_..."
            value={key}
            onChange={e => { setKey(e.target.value); setError('') }}
            autoFocus
            spellCheck={false}
          />
          {error && <p className={styles.error}>{error}</p>}
          <button className={styles.btn} type="submit" disabled={!key.trim()}>
            Start chatting
          </button>
        </form>
      </div>
    </div>
  )
}
