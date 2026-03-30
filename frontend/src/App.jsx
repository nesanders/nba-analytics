// Root component. Manages provider auth ({ provider, credential } in
// localStorage), the conversation message list, artifact list, and loading
// state. Renders ApiKeyModal on first visit or after a 401.
import { useState, useCallback, useRef } from 'react'
import ApiKeyModal from './components/ApiKeyModal.jsx'
import ChatThread from './components/ChatThread.jsx'
import ChatInput from './components/ChatInput.jsx'
import ArtifactTray from './components/ArtifactTray.jsx'
import { sendChat } from './api.js'
import styles from './App.module.css'

function loadAuth() {
  try {
    const raw = localStorage.getItem('nba_auth')
    if (raw) return JSON.parse(raw)
  } catch {}
  // Backwards-compat: migrate old plain groq key
  const legacy = localStorage.getItem('groq_key')
  if (legacy) return { provider: 'groq', credential: legacy }
  return null
}

export default function App() {
  const [auth, setAuth] = useState(loadAuth)
  const [messages, setMessages] = useState([])
  const [artifacts, setArtifacts] = useState([])
  const [loading, setLoading] = useState(false)
  const [focusedArtifactId, setFocusedArtifactId] = useState(null)
  const [highlightedArtifactId, setHighlightedArtifactId] = useState(null)
  const artifactCounter = useRef(0)
  const highlightTimer = useRef(null)

  const handleKeySet = useCallback((authObj) => {
    localStorage.setItem('nba_auth', JSON.stringify(authObj))
    localStorage.removeItem('groq_key') // remove legacy key if present
    setAuth(authObj)
  }, [])

  const handleClearKey = useCallback(() => {
    localStorage.removeItem('nba_auth')
    localStorage.removeItem('groq_key')
    setAuth(null)
  }, [])

  const handleArtifactClick = useCallback((artifactId) => {
    document.getElementById(`artifact-${artifactId}`)
      ?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    clearTimeout(highlightTimer.current)
    setHighlightedArtifactId(artifactId)
    highlightTimer.current = setTimeout(() => setHighlightedArtifactId(null), 1500)
    setFocusedArtifactId(artifactId)
  }, [])

  const handleSend = useCallback(async (text) => {
    const userMsg = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setLoading(true)

    const history = messages.map(m => ({ role: m.role, content: m.content }))

    try {
      const data = await sendChat(text, history, auth)

      let artifactId = null
      if (data.figure || data.sql) {
        artifactCounter.current += 1
        artifactId = artifactCounter.current
        setArtifacts(prev => [...prev, {
          id: artifactId,
          question: text,
          figure: data.figure || null,
          sql: data.sql || null,
        }])
      }

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: data.text,
        artifactId,
      }])
    } catch (err) {
      if (err.message === 'invalid_key') handleClearKey()
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: err.message === 'invalid_key'
          ? 'Your credentials were rejected. Please re-enter them.'
          : `Error: ${err.message}`,
        error: true,
      }])
    } finally {
      setLoading(false)
    }
  }, [messages, auth, handleClearKey])

  if (!auth) return <ApiKeyModal onKeySet={handleKeySet} />

  const providerLabel = auth.provider === 'gemini' ? 'Gemini 2.5 Flash' : 'Groq'

  return (
    <div className={styles.app}>
      <header className={styles.header}>
        <span className={styles.logo}>
          <img src="/logo.svg" alt="" style={{ height: '28px', verticalAlign: 'middle', marginRight: '8px' }} />
          NBA Analytics
        </span>
        <button className={styles.keyBtn} onClick={handleClearKey} title="Switch LLM provider or change API key">
          ⚙ {providerLabel} · Change
        </button>
      </header>
      <div className={styles.main}>
        <div className={styles.chatPanel}>
          <ChatThread
            messages={messages}
            loading={loading}
            onSend={handleSend}
            onArtifactClick={handleArtifactClick}
          />
          <ChatInput onSend={handleSend} disabled={loading} />
        </div>
        <div className={styles.artifactPanel}>
          <ArtifactTray
            artifacts={artifacts}
            focusedArtifactId={focusedArtifactId}
            highlightedArtifactId={highlightedArtifactId}
            onFocusChange={setFocusedArtifactId}
          />
        </div>
      </div>
    </div>
  )
}
