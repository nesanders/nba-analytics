// Single chat bubble. User messages are right-aligned; assistant messages are
// left-aligned. If the response produced an artifact (chart/SQL), a numbered
// badge links to it in the artifact tray on the right.
import styles from './Message.module.css'

export default function Message({ message, onArtifactClick }) {
  const isUser = message.role === 'user'

  return (
    <div className={`${styles.wrapper} ${isUser ? styles.user : styles.assistant}`}>
      <div className={`${styles.bubble} ${message.error ? styles.error : ''}`}>
        <p className={styles.text}>{message.content}</p>

        {message.artifactId != null && (
          <button
            className={styles.artifactLink}
            onClick={() => onArtifactClick(message.artifactId)}
          >
            ↗ Artifact #{message.artifactId}
          </button>
        )}
      </div>
    </div>
  )
}
