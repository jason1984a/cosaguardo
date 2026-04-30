let searchTimeout;

// Cache client-side — evita chiamate ripetute per la stessa query
const _searchCache = new Map();
const _CACHE_MAX = 80;

function escapeHtml(text) {
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function highlightMatch(text, query) {
    if (!query) return escapeHtml(text);
    const escapedText = escapeHtml(text);
    const escapedQuery = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const regex = new RegExp(`(${escapedQuery})`, "ig");
    return escapedText.replace(regex, "<mark>$1</mark>");
}

function scoreMovie(movie, query) {
    const q = query.toLowerCase().trim();
    const display = (movie.display_title || movie.title || "").toLowerCase();
    const raw = (movie.title || "").toLowerCase();
    if (!q) return 0;
    if (display === q || raw === q) return 100;
    if (display.startsWith(q) || raw.startsWith(q)) return 80;
    if (display.includes(q) || raw.includes(q)) return 60;
    if (display.split(/\s+/).some(w => w.startsWith(q))) return 40;
    return 10;
}

function renderSuggestions(suggestionsBox, data, currentQuery, input) {
    suggestionsBox.innerHTML = "";
    const ranked = [...data]
        .map(m => ({ ...m, _score: scoreMovie(m, currentQuery) }))
        .sort((a, b) => b._score - a._score)
        .slice(0, 8);

    ranked.forEach(movie => {
        const div = document.createElement("div");
        div.className = "suggestion-item";
        const displayText = movie.display_title || movie.title;
        div.innerHTML = highlightMatch(displayText, currentQuery);
        div.onclick = () => {
            input.value = movie.title;
            suggestionsBox.innerHTML = "";
        };
        suggestionsBox.appendChild(div);
    });
}

async function searchMovie(input) {
    const container = input.parentElement;
    const suggestionsBox = container.querySelector(".suggestions");
    const query = input.value.trim();

    clearTimeout(searchTimeout);

    if (query.length < 2) {
        suggestionsBox.innerHTML = "";
        return;
    }

    const selectedType = document.querySelector('input[name="content_type"]:checked')?.value || "movie";
    const cacheKey = `${selectedType}:${query.toLowerCase()}`;

    // Risposta istantanea dalla cache client
    if (_searchCache.has(cacheKey)) {
        renderSuggestions(suggestionsBox, _searchCache.get(cacheKey), query, input);
        return;
    }

    searchTimeout = setTimeout(async () => {
        const currentQuery = input.value.trim();
        if (currentQuery.length < 2) { suggestionsBox.innerHTML = ""; return; }

        const currentKey = `${selectedType}:${currentQuery.toLowerCase()}`;

        // Controlla ancora la cache (potrebbe essere arrivata nel frattempo)
        if (_searchCache.has(currentKey)) {
            renderSuggestions(suggestionsBox, _searchCache.get(currentKey), currentQuery, input);
            return;
        }

        try {
            // Usa /search-fast — solo DB locale, risposta immediata
            const res = await fetch(`/search-fast?q=${encodeURIComponent(currentQuery)}&content_type=${encodeURIComponent(selectedType)}`);
            const data = await res.json();

            if (input.value.trim() !== currentQuery) return;

            // Salva in cache
            if (_searchCache.size >= _CACHE_MAX) {
                _searchCache.delete(_searchCache.keys().next().value);
            }
            _searchCache.set(currentKey, data);

            renderSuggestions(suggestionsBox, data, currentQuery, input);
        } catch (e) {
            suggestionsBox.innerHTML = "";
        }
    }, 120);  // debounce 120ms invece di 180ms
}

document.addEventListener("click", function(event) {
    if (!event.target.closest(".input-group")) {
        document.querySelectorAll(".suggestions").forEach(box => {
            box.innerHTML = "";
        });
    }
});

document.addEventListener("DOMContentLoaded", function () {
    const form = document.getElementById("recommend-form");
    const submitBtn = document.getElementById("submit-btn");

    if (!form || !submitBtn) return;

    form.addEventListener("submit", function () {
        const btnText = submitBtn.querySelector(".btn-text");
        const btnLoading = submitBtn.querySelector(".btn-loading");
        submitBtn.disabled = true;
        if (btnText) btnText.style.display = "none";
        if (btnLoading) btnLoading.style.display = "inline";
        submitBtn.classList.add("is-loading");
    });
});
