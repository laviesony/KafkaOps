// API client for KafkaOps backend

const API_BASE = '/api';

// Types matching backend models
export interface Message {
    id: number;
    topic: string;
    partition: number;
    offset: number;
    key?: string;
    headers?: Record<string, string>;
    timestamp: number;
    decodedPayload?: unknown;
    decodeError?: string;
}

export interface APIResponse<T> {
    data?: T;
    error?: string;
    total?: number;
    page?: number;
    perPage?: number;
}

export interface ReplayResult {
    success: boolean;
    topic: string;
    partition: number;
    offset: number;
}

export interface BulkPreviewItem {
    messageId: number;
    before: unknown;
    after: unknown;
    error?: string;
}

export interface BulkPreviewResponse {
    previews: BulkPreviewItem[];
    errors?: string[];
}

export interface BulkExecuteResponse {
    succeeded: number;
    failed: number;
    errors?: string[];
}

export interface Patch {
    op: 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test';
    path: string;
    value?: unknown;
    from?: string;
}

class APIClient {
    private async fetch<T>(url: string, options?: RequestInit): Promise<T> {
        const response = await fetch(`${API_BASE}${url}`, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options?.headers,
            },
        });

        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }

        return response.json();
    }

    // Messages
    async getMessages(page = 1, limit = 50, topic?: string): Promise<APIResponse<Message[]>> {
        const params = new URLSearchParams({
            page: page.toString(),
            limit: limit.toString(),
        });
        if (topic) {
            params.set('topic', topic);
        }
        return this.fetch<APIResponse<Message[]>>(`/messages?${params}`);
    }

    async getMessage(id: number): Promise<APIResponse<Message>> {
        return this.fetch<APIResponse<Message>>(`/messages/${id}`);
    }

    async replayMessage(id: number, payload: unknown, topic?: string): Promise<APIResponse<ReplayResult>> {
        return this.fetch<APIResponse<ReplayResult>>(`/messages/${id}/replay`, {
            method: 'POST',
            body: JSON.stringify({ payload, topic }),
        });
    }

    // Bulk operations (PRO)
    async bulkPreview(messageIds: number[], patch: Patch[]): Promise<BulkPreviewResponse> {
        return this.fetch<BulkPreviewResponse>('/bulk/preview', {
            method: 'POST',
            body: JSON.stringify({ messageIds, patch }),
        });
    }

    async bulkExecute(messageIds: number[], patch: Patch[], confirmed = true): Promise<BulkExecuteResponse> {
        return this.fetch<BulkExecuteResponse>('/bulk/execute', {
            method: 'POST',
            body: JSON.stringify({ messageIds, patch, confirmed }),
        });
    }

    // Health check
    async health(): Promise<boolean> {
        try {
            const response = await fetch('/health');
            return response.ok;
        } catch {
            return false;
        }
    }
}

export const apiClient = new APIClient();
