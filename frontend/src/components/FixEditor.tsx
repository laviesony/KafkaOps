/*
FixEditor responsibilities:

- Render Monaco Editor
- Display JSON payload
- Validate JSON against schema-derived JSON Schema
- Show validation errors inline
- Disable Replay button until valid
*/

import { useState, useCallback, useEffect } from 'react';
import Editor from '@monaco-editor/react';
import { Message, apiClient, ReplayResult } from '../api/client';

interface FixEditorProps {
    message: Message | null;
    onReplaySuccess?: (result: ReplayResult) => void;
}

export function FixEditor({ message, onReplaySuccess }: FixEditorProps) {
    const [content, setContent] = useState('');
    const [isValid, setIsValid] = useState(true);
    const [parseError, setParseError] = useState<string | null>(null);
    const [replaying, setReplaying] = useState(false);
    const [replayResult, setReplayResult] = useState<{ success: boolean; message: string } | null>(null);

    // Update editor content when message changes
    useEffect(() => {
        if (message?.decodedPayload) {
            try {
                const formatted = JSON.stringify(message.decodedPayload, null, 2);
                setContent(formatted);
                setIsValid(true);
                setParseError(null);
                setReplayResult(null);
            } catch {
                setContent('');
                setIsValid(false);
            }
        } else {
            setContent('');
            setIsValid(false);
        }
    }, [message]);

    const handleEditorChange = useCallback((value: string | undefined) => {
        const newContent = value || '';
        setContent(newContent);
        setReplayResult(null);

        // Validate JSON
        try {
            JSON.parse(newContent);
            setIsValid(true);
            setParseError(null);
        } catch (err) {
            setIsValid(false);
            setParseError(err instanceof Error ? err.message : 'Invalid JSON');
        }
    }, []);

    const handleReplay = useCallback(async () => {
        if (!message || !isValid) return;

        setReplaying(true);
        setReplayResult(null);

        try {
            const payload = JSON.parse(content);
            const response = await apiClient.replayMessage(message.id, payload);

            if (response.data?.success) {
                setReplayResult({
                    success: true,
                    message: `Replayed to ${response.data.topic}:${response.data.partition}@${response.data.offset}`,
                });
                onReplaySuccess?.(response.data);
            } else {
                setReplayResult({
                    success: false,
                    message: response.error || 'Replay failed',
                });
            }
        } catch (err) {
            setReplayResult({
                success: false,
                message: err instanceof Error ? err.message : 'Replay failed',
            });
        } finally {
            setReplaying(false);
        }
    }, [message, content, isValid, onReplaySuccess]);

    const handleReset = useCallback(() => {
        if (message?.decodedPayload) {
            const formatted = JSON.stringify(message.decodedPayload, null, 2);
            setContent(formatted);
            setIsValid(true);
            setParseError(null);
            setReplayResult(null);
        }
    }, [message]);

    if (!message) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">✏️</div>
                <h2>Fix & Replay</h2>
                <p>Select a message to edit and replay.</p>
            </div>
        );
    }

    return (
        <div className="fix-editor">
            <div className="fix-editor-toolbar">
                <div className="flex gap-2" style={{ alignItems: 'center' }}>
                    <span style={{ fontSize: '14px', color: 'var(--text-secondary)' }}>
                        Editing: #{message.offset}
                    </span>
                    {parseError && (
                        <span className="badge badge-error">{parseError}</span>
                    )}
                    {isValid && !parseError && content && (
                        <span className="badge badge-success">Valid JSON</span>
                    )}
                </div>
                <button className="btn btn-secondary" onClick={handleReset}>
                    Reset
                </button>
            </div>

            <div className="fix-editor-monaco">
                <Editor
                    height="100%"
                    defaultLanguage="json"
                    value={content}
                    onChange={handleEditorChange}
                    theme="vs-dark"
                    options={{
                        minimap: { enabled: false },
                        fontSize: 13,
                        fontFamily: 'var(--font-mono)',
                        lineNumbers: 'on',
                        wordWrap: 'on',
                        automaticLayout: true,
                        scrollBeyondLastLine: false,
                        tabSize: 2,
                    }}
                />
            </div>

            <div className="fix-editor-actions">
                {replayResult && (
                    <div
                        className={`badge ${replayResult.success ? 'badge-success' : 'badge-error'}`}
                        style={{ marginRight: 'auto', padding: '8px 12px' }}
                    >
                        {replayResult.message}
                    </div>
                )}
                <button
                    className="btn btn-primary"
                    onClick={handleReplay}
                    disabled={!isValid || replaying || !content}
                >
                    {replaying ? (
                        <>
                            <span className="loading-spinner" style={{ width: 16, height: 16 }} />
                            Replaying...
                        </>
                    ) : (
                        '🚀 Replay Message'
                    )}
                </button>
            </div>
        </div>
    );
}
