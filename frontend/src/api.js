// Fetch wrapper for the backend API.
// Supports two providers via auth: { provider, credential }
//   Groq   → sends X-Groq-Key header (user-supplied key)
//   Gemini → sends X-Gemini-Token header (operator auth token)
// Throws 'invalid_key' on 401 so App can clear the credential and re-show the modal.
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080'

function authHeaders({ provider, credential }) {
  if (provider === 'gemini') return { 'X-Gemini-Token': credential }
  return { 'X-Groq-Key': credential }
}

export async function sendChat(message, history, auth) {
  const res = await fetch(`${API_URL}/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(auth),
    },
    body: JSON.stringify({ message, history }),
  })

  if (res.status === 401) throw new Error('invalid_key')
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `Server error ${res.status}`)
  }
  return res.json()
}

export async function fetchShotChart(playerId, season, auth) {
  const params = new URLSearchParams({ player_id: playerId, season })
  const res = await fetch(`${API_URL}/shot_chart?${params}`, {
    headers: authHeaders(auth),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `Server error ${res.status}`)
  }
  return res.json()
}
