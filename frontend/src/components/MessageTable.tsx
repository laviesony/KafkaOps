/*
MessageTable requirements:

- Render 100,000+ Kafka messages
- Must use @tanstack/react-virtual
- No full array rendering
- Rows have dynamic heights
- Selection must be keyboard-accessible
- Scroll must be smooth under load
*/

import { useRef, useCallback } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { Message } from '../api/client';

interface MessageTableProps {
    messages: Message[];
    selectedId: number | null;
    onSelect: (message: Message) => void;
    loading?: boolean;
}

export function MessageTable({ messages, selectedId, onSelect, loading }: MessageTableProps) {
    const parentRef = useRef<HTMLDivElement>(null);

    const rowVirtualizer = useVirtualizer({
        count: messages.length,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 56,
        overscan: 10,
    });

    const handleKeyDown = useCallback((e: React.KeyboardEvent, index: number) => {
        if (e.key === 'ArrowDown' && index < messages.length - 1) {
            e.preventDefault();
            onSelect(messages[index + 1]);
        } else if (e.key === 'ArrowUp' && index > 0) {
            e.preventDefault();
            onSelect(messages[index - 1]);
        } else if (e.key === 'Enter') {
            onSelect(messages[index]);
        }
    }, [messages, onSelect]);

    const formatTimestamp = (ts: number) => {
        return new Date(ts).toLocaleString();
    };

    const formatPreview = (payload: unknown): string => {
        if (!payload) return '—';
        if (typeof payload === 'string') return payload.slice(0, 100);
        try {
            return JSON.stringify(payload).slice(0, 100);
        } catch {
            return '—';
        }
    };

    if (loading) {
        return (
            <div className="empty-state">
                <div className="loading-spinner" />
                <p>Loading messages...</p>
            </div>
        );
    }

    if (messages.length === 0) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">📭</div>
                <h2>No Messages</h2>
                <p>No DLQ messages found. Configure a topic to start consuming.</p>
            </div>
        );
    }

    return (
        <div
            ref={parentRef}
            className="panel-content"
            style={{ overflow: 'auto' }}
        >
            <div
                className="message-table"
                style={{
                    height: `${rowVirtualizer.getTotalSize()}px`,
                    width: '100%',
                    position: 'relative',
                }}
            >
                {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const message = messages[virtualRow.index];
                    const isSelected = message.id === selectedId;

                    return (
                        <div
                            key={message.id}
                            className={`message-row ${isSelected ? 'selected' : ''}`}
                            style={{
                                position: 'absolute',
                                top: 0,
                                left: 0,
                                width: '100%',
                                height: `${virtualRow.size}px`,
                                transform: `translateY(${virtualRow.start}px)`,
                            }}
                            tabIndex={0}
                            onClick={() => onSelect(message)}
                            onKeyDown={(e) => handleKeyDown(e, virtualRow.index)}
                            role="button"
                            aria-selected={isSelected}
                        >
                            <span className="message-offset">#{message.offset}</span>
                            <span className="message-topic">{message.topic}</span>
                            <span className="message-timestamp">{formatTimestamp(message.timestamp)}</span>
                            <span className="message-preview">
                                {message.decodeError ? (
                                    <span className="badge badge-error">Decode Error</span>
                                ) : (
                                    formatPreview(message.decodedPayload)
                                )}
                            </span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
