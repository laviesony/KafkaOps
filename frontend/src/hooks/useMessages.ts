import { useState, useEffect, useCallback } from 'react';
import { apiClient, Message } from '../api/client';

interface UseMessagesResult {
    messages: Message[];
    loading: boolean;
    error: string | null;
    total: number;
    page: number;
    perPage: number;
    setPage: (page: number) => void;
    refetch: () => void;
}

export function useMessages(topic?: string, perPage = 50): UseMessagesResult {
    const [messages, setMessages] = useState<Message[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);

    const fetchMessages = useCallback(async () => {
        setLoading(true);
        setError(null);

        try {
            const response = await apiClient.getMessages(page, perPage, topic);
            setMessages(response.data || []);
            setTotal(response.total || 0);
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to fetch messages');
            setMessages([]);
        } finally {
            setLoading(false);
        }
    }, [page, perPage, topic]);

    useEffect(() => {
        fetchMessages();
    }, [fetchMessages]);

    return {
        messages,
        loading,
        error,
        total,
        page,
        perPage,
        setPage,
        refetch: fetchMessages,
    };
}
