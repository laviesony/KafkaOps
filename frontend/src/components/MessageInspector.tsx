import { Message } from '../api/client';

interface MessageInspectorProps {
    message: Message | null;
}

export function MessageInspector({ message }: MessageInspectorProps) {
    if (!message) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">👆</div>
                <h2>Select a Message</h2>
                <p>Choose a message from the list to view details.</p>
            </div>
        );
    }

    const formatTimestamp = (ts: number) => {
        return new Date(ts).toISOString();
    };

    const headers = message.headers || {};
    const headerEntries = Object.entries(headers);

    return (
        <div className="inspector">
            {/* Metadata Section */}
            <div className="inspector-section">
                <h3>Metadata</h3>
                <div className="inspector-field">
                    <span className="inspector-label">Topic</span>
                    <span className="inspector-value">{message.topic}</span>
                </div>
                <div className="inspector-field">
                    <span className="inspector-label">Partition</span>
                    <span className="inspector-value">{message.partition}</span>
                </div>
                <div className="inspector-field">
                    <span className="inspector-label">Offset</span>
                    <span className="inspector-value">{message.offset}</span>
                </div>
                <div className="inspector-field">
                    <span className="inspector-label">Timestamp</span>
                    <span className="inspector-value">{formatTimestamp(message.timestamp)}</span>
                </div>
                {message.key && (
                    <div className="inspector-field">
                        <span className="inspector-label">Key</span>
                        <span className="inspector-value">{message.key}</span>
                    </div>
                )}
            </div>

            {/* Headers Section */}
            {headerEntries.length > 0 && (
                <div className="inspector-section">
                    <h3>Headers</h3>
                    <div className="inspector-headers">
                        {headerEntries.map(([key, value]) => (
                            <div key={key} className="header-row">
                                <span className="header-key">{key}</span>
                                <span className="header-value">{value}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Decode Status */}
            {message.decodeError && (
                <div className="inspector-section">
                    <h3>Decode Error</h3>
                    <div className="badge badge-error">{message.decodeError}</div>
                </div>
            )}

            {/* Payload Preview */}
            <div className="inspector-section">
                <h3>Payload Preview</h3>
                <pre
                    style={{
                        background: 'var(--bg-tertiary)',
                        padding: '12px',
                        borderRadius: 'var(--radius-md)',
                        fontSize: '12px',
                        fontFamily: 'var(--font-mono)',
                        overflow: 'auto',
                        maxHeight: '200px',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                    }}
                >
                    {message.decodedPayload
                        ? JSON.stringify(message.decodedPayload, null, 2)
                        : '(No payload)'}
                </pre>
            </div>
        </div>
    );
}
