package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func NewRouter(h *Handlers) http.Handler {
	r := chi.NewRouter()
	r.Get("/health", h.Health)
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/tasks", h.EnqueueTask)
		r.Get("/tasks/{id}", h.GetTask)
		r.Post("/dags", h.SubmitDag)
	})
	return r
}
