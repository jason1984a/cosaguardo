let searchTimeout;

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

    const words = display.split(/\s+/);
    if (words.some(word => word.startsWith(q))) return 40;

    return 10;
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

    searchTimeout = setTimeout(async () => {
        const currentQuery = input.value.trim();

        if (currentQuery.length < 2) {
            suggestionsBox.innerHTML = "";
            return;
        }

        try {
            const selectedType = document.querySelector('input[name="content_type"]:checked')?.value || "movie";
	    const response = await fetch(`/search?q=${encodeURIComponent(currentQuery)}&content_type=${encodeURIComponent(selectedType)}`);
            const data = await response.json();

            if (input.value.trim() !== currentQuery) {
                return;
            }

            suggestionsBox.innerHTML = "";

            const ranked = [...data]
                .map(movie => ({
                    ...movie,
                    _score: scoreMovie(movie, currentQuery)
                }))
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
        } catch (error) {
            suggestionsBox.innerHTML = "";
        }
    }, 180);
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