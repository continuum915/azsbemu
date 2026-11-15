// SbBrowser's small browser-side helpers: theme switching, Ctrl+K to the filter box, dialog focus, and copy to clipboard.
// The theme is applied before Blazor starts by the inline script in App.razor, so the page never flashes.
window.sbBrowser = {
    theme() {
        return document.documentElement.dataset.theme || 'dark';
    },
    toggleTheme() {
        const next = this.theme() === 'dark' ? 'light' : 'dark';
        document.documentElement.dataset.theme = next;
        try { localStorage.setItem('sb-theme', next); } catch { /* storage may be unavailable */ }
        return next;
    },
    // Moves focus into a dialog: its first field, or the dialog itself. Never a destructive button, so a stray
    // Enter cannot confirm a purge.
    focusFirst(element) {
        if (!element) return;
        const field = element.querySelector('input:not([type=hidden]), select, textarea');
        (field || element).focus();
    },
    async copy(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch {
            return false;
        }
    },
};

document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && !e.altKey && e.key.toLowerCase() === 'k') {
        const filter = document.getElementById('sb-filter');
        if (filter) {
            e.preventDefault();
            filter.focus();
            filter.select();
        }
    }
});
