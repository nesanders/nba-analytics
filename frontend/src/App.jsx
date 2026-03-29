// Root component. Manages the Groq API key (localStorage), the conversation
// message list, artifact list, and loading state. Renders the ApiKeyModal on
// first visit or after a 401, and the two-panel chat+artifact UI once a key
// is present.
import { useState, useCallback, useRef } from 'react'
import ApiKeyModal from './components/ApiKeyModal.jsx'
import ChatThread from './components/ChatThread.jsx'
import ChatInput from './components/ChatInput.jsx'
import ArtifactTray from './components/ArtifactTray.jsx'
import { sendChat } from './api.js'
import styles from './App.module.css'

export default function App() {
  const [groqKey, setGroqKey] = useState(() => localStorage.getItem('groq_key') || '')
  const [messages, setMessages] = useState([])
  const [artifacts, setArtifacts] = useState([])
  const [loading, setLoading] = useState(false)
  const [focusedArtifactId, setFocusedArtifactId] = useState(null)
  const [highlightedArtifactId, setHighlightedArtifactId] = useState(null)
  const artifactCounter = useRef(0)
  const highlightTimer = useRef(null)

  const handleKeySet = useCallback((key) => {
    localStorage.setItem('groq_key', key)
    setGroqKey(key)
  }, [])

  const handleClearKey = useCallback(() => {
    localStorage.removeItem('groq_key')
    setGroqKey('')
  }, [])

  // Called when the user clicks "↗ Artifact #N" in a chat bubble.
  // Scrolls the tray card into view, flashes it, and opens the overlay.
  const handleArtifactClick = useCallback((artifactId) => {
    document.getElementById(`artifact-${artifactId}`)
      ?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })

    // Flash highlight for 1.5s
    clearTimeout(highlightTimer.current)
    setHighlightedArtifactId(artifactId)
    highlightTimer.current = setTimeout(() => setHighlightedArtifactId(null), 1500)

    // Open overlay
    setFocusedArtifactId(artifactId)
  }, [])

  const handleSend = useCallback(async (text) => {
    const userMsg = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setLoading(true)

    const history = messages.map(m => ({ role: m.role, content: m.content }))

    try {
      const data = await sendChat(text, history, groqKey)

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
          ? 'Your Groq API key was rejected. Please enter a valid key.'
          : `Error: ${err.message}`,
        error: true,
      }])
    } finally {
      setLoading(false)
    }
  }, [messages, groqKey, handleClearKey])

  if (!groqKey) return <ApiKeyModal onKeySet={handleKeySet} />

  return (
    <div className={styles.app}>
      <header className={styles.header}>
        <span className={styles.logo}>🏀 NBA Analytics</span>
        <button className={styles.keyBtn} onClick={handleClearKey} title="Change API key">
          API Key
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
