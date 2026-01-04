import { useState, useCallback } from 'react';
import { useMessages } from '../hooks/useMessages';
import { MessageTable } from '../components/MessageTable';
import { MessageInspector } from '../components/MessageInspector';
import { FixEditor } from '../components/FixEditor';
import { Message } from '../api/client';

export function DlqView() {
    const { messages, loading, error, total, page, perPage, setPage, refetch } = useMessages();
    const [selectedMessage, setSelectedMessage] = useState<Message | null>(null);

    const handleSelectMessage = useCallback((message: Message) => {
        setSelectedMessage(message);
    }, []);

    const handleReplaySuccess = useCallback(() => {
        // Optionally refresh or show notification
        refetch();
    }, [refetch]);

    const totalPages = Math.ceil(total / perPage);

    return (
        <div className="dlq-view">
            {/* Message List Panel */}
            <div className="panel dlq-list-panel">
                <div className="panel-header">
                    <span>DLQ Messages</span>
                    <div className="flex gap-2" style={{ alignItems: 'center' }}>
                        <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                            {total.toLocaleString()} messages
                        </span>
                        <button className="btn btn-secondary" onClick={refetch} style={{ padding: '4px 8px' }}>
                            ↻
                        </button>
                    </div>
                </div>

                {error ? (
                    <div className="empty-state">
                        <div className="badge badge-error">{error}</div>
                        <button className="btn btn-secondary" onClick={refetch} style={{ marginTop: '16px' }}>
                            Retry
                        </button>
                    </div>
                ) : (
                    <MessageTable
                        messages={messages}
                        selectedId={selectedMessage?.id || null}
                        onSelect={handleSelectMessage}
                        loading={loading}
                    />
                )}

                {/* Pagination */}
                {totalPages > 1 && (
                    <div
                        style={{
                            display: 'flex',
                            justifyContent: 'center',
                            gap: '8px',
                            padding: '12px',
                            borderTop: '1px solid var(--border-color)',
                        }}
                    >
                        <button
                            className="btn btn-secondary"
                            onClick={() => setPage(Math.max(1, page - 1))}
                            disabled={page <= 1}
                            style={{ padding: '6px 12px', fontSize: '12px' }}
                        >
                            ←
                        </button>
                        <span style={{ padding: '6px 12px', fontSize: '12px', color: 'var(--text-muted)' }}>
                            Page {page} of {totalPages}
                        </span>
                        <button
                            className="btn btn-secondary"
                            onClick={() => setPage(Math.min(totalPages, page + 1))}
                            disabled={page >= totalPages}
                            style={{ padding: '6px 12px', fontSize: '12px' }}
                        >
                            →
                        </button>
                    </div>
                )}
            </div>

            {/* Detail Panel */}
            <div className="dlq-detail-panel">
                {/* Inspector */}
                <div className="panel dlq-inspector-panel">
                    <div className="panel-header">Message Details</div>
                    <div className="panel-content">
                        <MessageInspector message={selectedMessage} />
                    </div>
                </div>

                {/* Editor */}
                <div className="panel dlq-editor-panel">
                    <div className="panel-header">Fix & Replay</div>
                    <FixEditor message={selectedMessage} onReplaySuccess={handleReplaySuccess} />
                </div>
            </div>
        </div>
    );
}
