import { DlqView } from './pages/DlqView';

function App() {
    return (
        <div className="app">
            <header className="app-header">
                <div className="app-logo">
                    <div className="app-logo-icon">K</div>
                    <h1>KafkaOps</h1>
                </div>
                <nav className="flex gap-4" style={{ alignItems: 'center' }}>
                    <span
                        style={{
                            fontSize: '12px',
                            color: 'var(--text-muted)',
                            padding: '4px 8px',
                            background: 'var(--bg-tertiary)',
                            borderRadius: 'var(--radius-sm)',
                        }}
                    >
                        Local Mode
                    </span>
                </nav>
            </header>
            <main className="app-main">
                <DlqView />
            </main>
        </div>
    );
}

export default App;
