// Full-screen modal shown on first visit or after key rejection.
// Two provider modes:
//   Groq  — user pastes their own free API key (starts with "gsk_")
//   Gemini — user enters the private auth token set by the operator;
//            the actual Gemini API key is server-side and never exposed.
import { useState } from 'react'
import styles from './ApiKeyModal.module.css'

export default function ApiKeyModal({ onKeySet }) {
  const [provider, setProvider] = useState('groq')
  const [credential, setCredential] = useState('')
  const [error, setError] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    const trimmed = credential.trim()
    if (!trimmed) return

    if (provider === 'groq' && !trimmed.startsWith('gsk_')) {
      setError('Groq API keys start with "gsk_". Get one free at console.groq.com.')
      return
    }

    onKeySet({ provider, credential: trimmed })
  }

  const handleProviderChange = (p) => {
    setProvider(p)
    setCredential('')
    setError('')
  }

  return (
    <div className={styles.overlay}>
      <div className={styles.modal}>
        <h1 className={styles.title}>🏀 NBA Analytics</h1>

        <div className={styles.tabs}>
          <button
            className={`${styles.tab} ${provider === 'groq' ? styles.tabActive : ''}`}
            onClick={() => handleProviderChange('groq')}
            type="button"
          >
            Groq (free)
          </button>
          <button
            className={`${styles.tab} ${provider === 'gemini' ? styles.tabActive : ''}`}
            onClick={() => handleProviderChange('gemini')}
            type="button"
          >
            Gemini
          </button>
        </div>

        {provider === 'groq' ? (
          <p className={styles.desc}>
            Paste your free{' '}
            <a href="https://console.groq.com/keys" target="_blank" rel="noreferrer">
              Groq API key
            </a>{' '}
            to use Llama 3.3 70B. Your key is stored only in your browser and
            never sent to our servers.
          </p>
        ) : (
          <p className={styles.desc}>
            Enter the Gemini access token to use Gemini 2.0 Flash via the
            server-side API key. The token is stored only in your browser.
          </p>
        )}

        <form onSubmit={handleSubmit} className={styles.form}>
          <input
            className={styles.input}
            type="password"
            placeholder={provider === 'groq' ? 'gsk_...' : 'Access token'}
            value={credential}
            onChange={e => { setCredential(e.target.value); setError('') }}
            autoFocus
            spellCheck={false}
          />
          {error && <p className={styles.error}>{error}</p>}
          <button className={styles.btn} type="submit" disabled={!credential.trim()}>
            Start chatting
          </button>
        </form>
      </div>
    </div>
  )
}
