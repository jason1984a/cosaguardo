
document.addEventListener("DOMContentLoaded", () => {
  const actions = document.querySelectorAll(".feedback-actions");

  actions.forEach(section => {
    section.querySelectorAll(".feedback-btn").forEach(button => {
      button.addEventListener("click", async (e) => {
        e.stopPropagation();
        const feedbackType = button.dataset.feedback;
        const title = section.dataset.title;
        const contentType = section.dataset.type;

        // stato prima del click
        const wasActive = button.classList.contains("active");

        // rimuove stato attivo da tutti i bottoni
        section.querySelectorAll(".feedback-btn").forEach(b => b.classList.remove("active"));

        // se non era attivo → attivalo subito (optimistic update)
        if (!wasActive) {
          button.classList.add("active");
        }

        // invia al backend
        try {
          const res = await fetch("/feedback", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              title: title,
              content_type: contentType,
              feedback_type: feedbackType
            })
          });

          const data = await res.json();

          // se errore → rollback visivo
          if (data.status !== "ok") {
            if (wasActive) button.classList.add("active");
            else button.classList.remove("active");
            alert("Errore nel salvataggio del feedback");
          }
        } catch (e) {
          // se fallisce la rete → rollback visivo
          if (wasActive) button.classList.add("active");
          else button.classList.remove("active");
          alert("Connessione non riuscita");
        }
      });
    });
  });
});
