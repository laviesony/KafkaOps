/*
Bulk remediation (PRO feature – FEATURE FLAGGED)

Responsibilities:
- Accept RFC 6902 JSON Patch definitions
- Apply patch to multiple selected messages
- Preview diffs before execution
- Execute replay in controlled batches
- Must NEVER auto-run without explicit user confirmation

Rules:
- Feature must be disabled unless Pro license is active
- UI must show "Preview → Confirm → Execute" steps
- Free tier must not access this component
*/

import { useState, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import { apiClient, Patch, BulkPreviewItem } from '../api/client';

interface BulkPatchPanelProps {
    selectedMessageIds: number[];
    onExecuteSuccess?: () => void;
}

export function BulkPatchPanel({ selectedMessageIds, onExecuteSuccess }: BulkPatchPanelProps) {
    const [patchJson, setPatchJson] = useState('[\n  { "op": "replace", "path": "/fieldName", "value": "newValue" }\n]');
    const [isValidPatch, setIsValidPatch] = useState(true);
    const [step, setStep] = useState<'edit' | 'preview' | 'confirm'>('edit');
    const [previews, setPreviews] = useState<BulkPreviewItem[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [result, setResult] = useState<{ succeeded: number; failed: number } | null>(null);

    const handlePatchChange = useCallback((value: string | undefined) => {
        const newValue = value || '';
        setPatchJson(newValue);
        setError(null);

        try {
            const parsed = JSON.parse(newValue);
            if (!Array.isArray(parsed)) {
                setIsValidPatch(false);
                return;
            }
            setIsValidPatch(true);
        } catch {
            setIsValidPatch(false);
        }
    }, []);

    const handlePreview = useCallback(async () => {
        if (!isValidPatch || selectedMessageIds.length === 0) return;

        setLoading(true);
        setError(null);

        try {
            const patches: Patch[] = JSON.parse(patchJson);
            const response = await apiClient.bulkPreview(selectedMessageIds, patches);
            setPreviews(response.previews);
            setStep('preview');
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Preview failed');
        } finally {
            setLoading(false);
        }
    }, [isValidPatch, selectedMessageIds, patchJson]);

    const handleExecute = useCallback(async () => {
        if (!isValidPatch || selectedMessageIds.length === 0) return;

        setLoading(true);
        setError(null);

        try {
            const patches: Patch[] = JSON.parse(patchJson);
            const response = await apiClient.bulkExecute(selectedMessageIds, patches, true);
            setResult({ succeeded: response.succeeded, failed: response.failed });
            setStep('confirm');
            if (response.succeeded > 0) {
                onExecuteSuccess?.();
            }
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Execution failed');
        } finally {
            setLoading(false);
        }
    }, [isValidPatch, selectedMessageIds, patchJson, onExecuteSuccess]);

    const handleReset = useCallback(() => {
        setStep('edit');
        setPreviews([]);
        setResult(null);
        setError(null);
    }, []);

    if (selectedMessageIds.length === 0) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">🔧</div>
                <h2>Bulk Patch (PRO)</h2>
                <p>Select multiple messages to apply bulk patches.</p>
            </div>
        );
    }

    return (
        <div className="fix-editor">
            <div className="fix-editor-toolbar">
                <div className="flex gap-2" style={{ alignItems: 'center' }}>
                    <span style={{ fontSize: '14px', color: 'var(--text-secondary)' }}>
                        {selectedMessageIds.length} messages selected
                    </span>
                    <span className="badge badge-warning">PRO</span>
                </div>
                <div className="flex gap-2">
                    {step !== 'edit' && (
                        <button className="btn btn-secondary" onClick={handleReset}>
                            ← Back
                        </button>
                    )}
                </div>
            </div>

            {step === 'edit' && (
                <>
                    <div className="fix-editor-monaco">
                        <Editor
                            height="100%"
                            defaultLanguage="json"
                            value={patchJson}
                            onChange={handlePatchChange}
                            theme="vs-dark"
                            options={{
                                minimap: { enabled: false },
                                fontSize: 13,
                                lineNumbers: 'on',
                                wordWrap: 'on',
                                automaticLayout: true,
                                scrollBeyondLastLine: false,
                                tabSize: 2,
                            }}
                        />
                    </div>

                    <div className="fix-editor-actions">
                        {error && (
                            <span className="badge badge-error" style={{ marginRight: 'auto' }}>
                                {error}
                            </span>
                        )}
                        <button
                            className="btn btn-primary"
                            onClick={handlePreview}
                            disabled={!isValidPatch || loading}
                        >
                            {loading ? 'Loading...' : 'Preview Changes →'}
                        </button>
                    </div>
                </>
            )}

            {step === 'preview' && (
                <>
                    <div className="panel-content" style={{ padding: '16px', overflow: 'auto' }}>
                        <h3 style={{ marginBottom: '16px', color: 'var(--text-secondary)' }}>
                            Preview ({previews.length} messages)
                        </h3>
                        {previews.map((preview) => (
                            <div
                                key={preview.messageId}
                                style={{
                                    marginBottom: '16px',
                                    padding: '12px',
                                    background: 'var(--bg-tertiary)',
                                    borderRadius: 'var(--radius-md)',
                                }}
                            >
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '8px' }}>
                                    Message #{preview.messageId}
                                </div>
                                {preview.error ? (
                                    <span className="badge badge-error">{preview.error}</span>
                                ) : (
                                    <pre style={{ fontSize: '11px', fontFamily: 'var(--font-mono)', overflow: 'auto' }}>
                                        {JSON.stringify(preview.after, null, 2)}
                                    </pre>
                                )}
                            </div>
                        ))}
                    </div>

                    <div className="fix-editor-actions">
                        <button className="btn btn-danger" onClick={handleExecute} disabled={loading}>
                            {loading ? 'Executing...' : '⚠️ Execute Bulk Replay'}
                        </button>
                    </div>
                </>
            )}

            {step === 'confirm' && result && (
                <div className="empty-state">
                    <div className="empty-state-icon">✅</div>
                    <h2>Bulk Operation Complete</h2>
                    <p>
                        <span className="badge badge-success">{result.succeeded} succeeded</span>
                        {result.failed > 0 && (
                            <span className="badge badge-error" style={{ marginLeft: '8px' }}>
                                {result.failed} failed
                            </span>
                        )}
                    </p>
                    <button className="btn btn-primary" onClick={handleReset} style={{ marginTop: '16px' }}>
                        Start New Batch
                    </button>
                </div>
            )}
        </div>
    );
}
