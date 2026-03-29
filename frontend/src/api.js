// Fetch wrapper for the backend API.
// Injects the X-Groq-Key header on every request; throws 'invalid_key' on 401
// so the app can clear the stored key and show the modal again.
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080'

export async function sendChat(message, history, groqKey) {
  const res = await fetch(`${API_URL}/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Groq-Key': groqKey,
    },
    body: JSON.stringify({ message, history }),
  })

  if (res.status === 401) {
    throw new Error('invalid_key')
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `Server error ${res.status}`)
  }

  return res.json()
}

export async function fetchShotChart(playerId, season, groqKey) {
  const params = new URLSearchParams({ player_id: playerId, season })
  const res = await fetch(`${API_URL}/shot_chart?${params}`, {
    headers: { 'X-Groq-Key': groqKey },
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `Server error ${res.status}`)
  }
  return res.json()
}
